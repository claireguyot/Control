/*
 * === This file is part of ALICE O² ===
 *
 * Copyright 2017-2018 CERN and copyright holders of ALICE O².
 * Author: Teo Mrnjavac <teo.mrnjavac@cern.ch>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * In applying this license CERN does not waive the privileges and
 * immunities granted to it by virtue of its status as an
 * Intergovernmental Organization or submit itself to any jurisdiction.
 */

package environment

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/AliceO2Group/Control/common/controlmode"
	"github.com/AliceO2Group/Control/common/event"
	"github.com/AliceO2Group/Control/common/utils"
	"github.com/AliceO2Group/Control/common/utils/uid"
	"github.com/AliceO2Group/Control/core/task"
	"github.com/AliceO2Group/Control/core/task/taskop"
	"github.com/AliceO2Group/Control/core/workflow"
	pb "github.com/AliceO2Group/Control/executor/protos"
	"github.com/sirupsen/logrus"
)

type Manager struct {
	mu                      sync.RWMutex
	m                       map[uid.ID]*Environment
	taskman                 *task.Manager
	incomingEventCh         <-chan event.Event
	pendingTeardownsCh      map[uid.ID]chan *event.TasksReleasedEvent
	pendingStateChangeCh    map[uid.ID]chan *event.TasksStateChangedEvent
}

func NewEnvManager(tm *task.Manager, incomingEventCh <-chan event.Event) *Manager {
	envman := &Manager{
		m:               make(map[uid.ID]*Environment),
		taskman:         tm,
		incomingEventCh: incomingEventCh,
		pendingTeardownsCh: make(map[uid.ID]chan *event.TasksReleasedEvent),
		pendingStateChangeCh: make(map[uid.ID]chan *event.TasksStateChangedEvent),
	}

	go func() {
		for ;; {
			select {
			case incomingEvent := <- envman.incomingEventCh:
				switch typedEvent := incomingEvent.(type) {
				case event.DeviceEvent:
					envman.handleDeviceEvent(typedEvent)
				case *event.TasksReleasedEvent:
					// If we got a TasksReleasedEvent, it must be matched with a pending
					// environment teardown.
					if thisEnvCh, ok := envman.pendingTeardownsCh[typedEvent.GetEnvironmentId()]; ok {
						thisEnvCh <- typedEvent
						close(thisEnvCh)
						delete(envman.pendingTeardownsCh, typedEvent.GetEnvironmentId())
					}
				case *event.TasksStateChangedEvent:
					// If we got a TasksStateChangedEvent, it must be matched with a pending
					// environment transition.
					if thisEnvCh, ok := envman.pendingStateChangeCh[typedEvent.GetEnvironmentId()]; ok {
						thisEnvCh <- typedEvent
					}
				default:
					// noop
				}
			}
		}
	}()
	return envman
}

func (envs *Manager) CreateEnvironment(workflowPath string, userVars map[string]string) (uid.ID, error) {
	envs.mu.Lock()

	// userVar identifiers come in 2 forms:
	// environment user var: "someKey"
	// workflow user var:    "path.to.some.role:someKey"
	// We need to split them into 2 structures, the first of which is passed to newEnvironment and the other one
	// to loadWorkflow as its keys must be injected into one or more specific roles.

	envUserVars := make(map[string]string)
	workflowUserVars := make(map[string]string)
	for k, v := range userVars {
		// If the key contains a ':', means we have a var associated with a specific workflow role
		if strings.ContainsRune(k, task.TARGET_SEPARATOR_RUNE) {
			workflowUserVars[k] = v
		} else {
			envUserVars[k] = v
		}
	}

	env, err := newEnvironment(envUserVars)
	if err != nil {
		envs.mu.Unlock()
		return uid.NilID(), err
	}
	env.hookHandlerF = func(hooks task.Tasks) error {
		return envs.taskman.TriggerHooks(hooks)
	}

	// Ensure the environment_id is available to all
	env.UserVars.Set("environment_id", env.id.String())

	env.workflow, err = envs.loadWorkflow(workflowPath, env.wfAdapter, workflowUserVars)
	if err != nil {
		err = fmt.Errorf("cannot load workflow template: %w", err)

		envs.mu.Unlock()
		return env.id, err
	}

	envs.m[env.id] = env
	envs.pendingStateChangeCh[env.id] = env.stateChangedCh

	err = env.TryTransition(NewConfigureTransition(
		envs.taskman,
		nil, //roles,
		nil,
		true),
	)
	envs.mu.Unlock()

	if err == nil {
		// CONFIGURE transition successful!
		env.subscribeToWfState(envs.taskman)

		return env.id, err
	}

	// Deployment/configuration failure code path starts here

	envState := env.CurrentState()
	log.WithField("state", envState).
		WithField("environment", env.Id().String()).
		WithError(err).
		Warn("environment deployment and configuration failed, cleanup in progress")

	envTasks := env.Workflow().GetTasks()
	// TeardownEnvironment manages the envs.mu internally
	// We do not get the error here cause it overwrites the failed deployment error
	// with <nil> which results to server.go to report back
	// cannot get newly created environment: no environment with id <env id>
	_ = envs.TeardownEnvironment(env.Id(), true/*force*/)

	killedTasks, _, rlsErr := envs.taskman.KillTasks(envTasks.GetTaskIds())
	if rlsErr != nil {
		log.WithError(rlsErr).Warn("task teardown error")
	}
	log.WithFields(logrus.Fields{
		"killedCount": len(killedTasks),
		"lastEnvState": envState,
	}).
	Warn("environment deployment failed, tasks were cleaned up")

	return env.id, err
}

func (envs *Manager) TeardownEnvironment(environmentId uid.ID, force bool) error {
	envs.mu.Lock()
	defer envs.mu.Unlock()

	env, err := envs.environment(environmentId)
	if err != nil {
		return err
	}

	if env.CurrentState() != "STANDBY" && !force {
		return errors.New(fmt.Sprintf("cannot teardown environment in state %s", env.CurrentState()))
	}

	tasksToRelease := env.Workflow().GetTasks()

	// we gather all DESTROY/after_DESTROY hooks, as these require special treatment
	destroyHooks := env.Workflow().GetHooksForTrigger("DESTROY")
	destroyHooks = append(destroyHooks, env.Workflow().GetHooksForTrigger("after_DESTROY")...)

	// for each found DESTROY hook,
	//     for each of *all* tasks last-to-first
	//         if the pointed task is one of the known cleanup hooks, remove it
	for _, hook := range destroyHooks {
		for i := len(tasksToRelease)-1; i >= 0; i-- {
			if hook == tasksToRelease[i] {
				tasksToRelease = append(tasksToRelease[:i], tasksToRelease[i+1:]...)
			}
		}
	}

	// we kill all tasks that aren't cleanup hooks
	taskmanMessage := task.NewEnvironmentMessage(taskop.ReleaseTasks, environmentId, tasksToRelease, nil)
	// close state channel
	if ch := envs.pendingStateChangeCh[environmentId]; ch != nil {
		close(envs.pendingStateChangeCh[environmentId])
	}
	delete(envs.pendingStateChangeCh, environmentId)
	


	// We set all callRoles to INACTIVE right now, because there's no task activation for them.
	// This is the callRole equivalent of AcquireTasks, which only pushes updates to taskRoles.
	allHooks := env.Workflow().GetHooksForTrigger("")	// no trigger = all hooks
	callHooks := allHooks.FilterCalls()							// get the calls
	if len(callHooks) > 0 {
		for _, h := range callHooks {
			pr, ok := h.GetParentRole().(workflow.PublicUpdatable)
			if !ok {
				continue
			}
			go pr.UpdateStatus(task.INACTIVE)
		}
	}

	pendingCh := make(chan *event.TasksReleasedEvent)
	envs.pendingTeardownsCh[environmentId] = pendingCh
	envs.mu.Unlock()
	envs.taskman.MessageChannel <- taskmanMessage

	incomingEv := <- pendingCh

	envs.mu.Lock()
	// If some tasks failed to release
	if taskReleaseErrors := incomingEv.GetTaskReleaseErrors(); len(taskReleaseErrors) > 0 {
		for taskId, err := range taskReleaseErrors {
			log.WithFields(logrus.Fields{
					"taskId": taskId,
					"environmentId": environmentId,
				}).
				WithError(err).
				Warn("task failed to release")
		}
		err = fmt.Errorf("%d tasks failed to release for environment %s",
			len(taskReleaseErrors), environmentId)
		return err
	}

	envs.mu.Unlock()
	// we trigger all cleanup hooks
	destroyHooks.FilterCalls().CallAll()
	cleanupTaskHooks := destroyHooks.FilterTasks()
	err = envs.taskman.TriggerHooks(cleanupTaskHooks)
	if err != nil {
		log.WithError(err).Warn("environment post-destroy hooks failed")
	}

	envs.mu.Lock()

	// and then we kill them too
	taskmanMessage = task.NewEnvironmentMessage(taskop.ReleaseTasks, environmentId, cleanupTaskHooks, nil)

	// we remake the pending teardown channel too, because each completed TasksReleasedEvent
	// automatically closes it
	pendingCh = make(chan *event.TasksReleasedEvent)
	envs.pendingTeardownsCh[environmentId] = pendingCh
	envs.mu.Unlock()
	envs.taskman.MessageChannel <- taskmanMessage

	incomingEv = <- pendingCh

	envs.mu.Lock()
	// If some cleanup hooks failed to release
	if taskReleaseErrors := incomingEv.GetTaskReleaseErrors(); len(taskReleaseErrors) > 0 {
		for taskId, err := range taskReleaseErrors {
			log.WithFields(logrus.Fields{
				"taskId": taskId,
				"environmentId": environmentId,
			}).
			WithError(err).
			Warn("task failed to release")
		}
		err = fmt.Errorf("%d tasks failed to release for environment %s",
			len(taskReleaseErrors), environmentId)
		return err
	}

	env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Message: "teardown complete", State: "DONE"})
	delete(envs.m, environmentId)
	env.unsubscribeFromWfState()
	return err
}

/*func (envs *Manager) Configuration(environmentId uuid.UUID) EnvironmentCfg {
	envs.mu.RLock()
	defer envs.mu.RUnlock()
	return envs.m[environmentId.Array()].cfg
}*/

func (envs *Manager) Ids() (keys []uid.ID) {
	envs.mu.RLock()
	defer envs.mu.RUnlock()
	keys = make([]uid.ID, len(envs.m))
	i := 0
	for k := range envs.m {
		keys[i] = k
		i++
	}
	return
}

func (envs *Manager) Environment(environmentId uid.ID) (env *Environment, err error) {
	envs.mu.RLock()
	defer envs.mu.RUnlock()
	return envs.environment(environmentId)
}

func (envs *Manager) environment(environmentId uid.ID) (env *Environment, err error) {
	if len(environmentId) == 0 { // invalid id
		return nil, fmt.Errorf("invalid id: %s", environmentId)
	}
	env, ok := envs.m[environmentId]
	if !ok {
		err = errors.New(fmt.Sprintf("no environment with id %s", environmentId))
	}
	return
}

func (envs *Manager) loadWorkflow(workflowPath string, parent workflow.Updatable, workflowUserVars map[string]string) (root workflow.Role, err error) {
	if strings.Contains(workflowPath, "://") {
		return nil, errors.New("workflow loading from file not implemented yet")
	}
	return workflow.Load(workflowPath, parent, envs.taskman, workflowUserVars)
}

func (envs *Manager) handleDeviceEvent(evt event.DeviceEvent) {
	if evt == nil {
		log.Error("cannot handle null DeviceEvent")
		return
	}

	switch evt.GetType() {
	case pb.DeviceEventType_BASIC_TASK_TERMINATED:
		if btt, ok := evt.(*event.BasicTaskTerminated); ok {
			log.WithPrefix("scheduler").
				WithFields(logrus.Fields{
					"exitCode": btt.ExitCode,
					"stdout": btt.Stdout,
					"stderr": btt.Stderr,
					"finalMesosState": btt.FinalMesosState.String(),
				}).
				Info("basic task terminated")

			// Propagate this information to the task/role
			taskId := evt.GetOrigin().TaskId
			t := envs.taskman.GetTask(taskId.Value)
			isHook := false
			if t != nil {
				t.SendEvent(&event.TaskEvent{Name: t.GetName(), TaskID: taskId.Value, Status: btt.FinalMesosState.String(), Hostname: t.GetHostname() , ClassName: t.GetClassName()})
				if parentRole, ok := t.GetParentRole().(workflow.Role); ok {
					parentRole.SetRuntimeVars(map[string]string{
						"taskResult.exitCode": strconv.Itoa(btt.ExitCode),
						"taskResult.stdout": btt.Stdout,
						"taskResult.stderr": btt.Stderr,
						"taskResult.finalStatus": btt.FinalMesosState.String(),
						"taskResult.timestamp": utils.NewUnixTimestamp(),
					})

					// If it's an update following a HOOK execution
					if t.GetControlMode() == controlmode.HOOK {
						isHook = true
						env, err := envs.environment(t.GetEnvironmentId())
						if err != nil {
							log.WithPrefix("scheduler").WithError(err).Error("cannot find environment for DeviceEvent")
						}
						env.NotifyEvent(evt)
					}
				} else {
					log.WithPrefix("scheduler").Error("DeviceEvent BASIC_TASK_TERMINATED received for task with no parent role")
				}
			} else {
				log.WithPrefix("scheduler").Debug("cannot find task for DeviceEvent BASIC_TASK_TERMINATED")
			}

			// If the task hasn't already been killed
			// AND it's not a hook
			if !isHook {
				goto doFallthrough
			}
		}
		return
	doFallthrough:
		fallthrough
	case pb.DeviceEventType_END_OF_STREAM:
		taskId := evt.GetOrigin().TaskId
		t := envs.taskman.GetTask(taskId.Value)
		if t == nil {
			log.WithPrefix("scheduler").Debug("cannot find task for DeviceEvent BASIC_TASK_TERMINATED")
			return
		}
		env, err := envs.environment(t.GetEnvironmentId())
		if err != nil {
			log.WithPrefix("scheduler").WithError(err).Error("cannot find environment for DeviceEvent")
		}
		if env.CurrentState() == "RUNNING" {
			t.SetSafeToStop(true) // we mark this specific task as ok to STOP
			go func() {
				if env.IsSafeToStop() {     // but then we ask the env whether *all* of them are
					err = env.TryTransition(NewStopActivityTransition(envs.taskman))
					if err != nil {
						log.WithPrefix("scheduler").WithError(err).Error("cannot stop run after END_OF_STREAM event")
					}
				}
			}()
		}
	}
}

func (envs *Manager) CreateAutoEnvironment(workflowPath string, userVars map[string]string, sub Subscription) {

	envUserVars := make(map[string]string)
	workflowUserVars := make(map[string]string)
	for k, v := range userVars {
		if strings.ContainsRune(k, task.TARGET_SEPARATOR_RUNE) {
			workflowUserVars[k] = v
		} else {
			envUserVars[k] = v
		}
	}

	env, err := newEnvironment(envUserVars)
	if err != nil {
		env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
		return
	}
	env.addSubscription(sub)
	defer env.closeStream()
	env.hookHandlerF = func(hooks task.Tasks) error {
		return envs.taskman.TriggerHooks(hooks)
	}
	env.workflow, err = envs.loadWorkflow(workflowPath, env.wfAdapter, workflowUserVars)
	if err != nil {
		err = fmt.Errorf("cannot load workflow template: %w", err)
		env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
		return
	}

	envs.mu.Lock()
	envs.m[env.id] = env
	envs.pendingStateChangeCh[env.id] = env.stateChangedCh
	envs.mu.Unlock()

	err = env.TryTransition(NewConfigureTransition(
		envs.taskman,
		nil, //roles,
		nil,
		true	))
	if err != nil {
		envState := env.CurrentState()
		env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
		log.WithField("state", envState).
			WithField("environment", env.Id().String()).
			WithError(err).
			Warn("environment deployment and configuration failed, cleanup in progress")

		envTasks := env.Workflow().GetTasks()
		// TeardownEnvironment manages the envs.mu internally
		err = envs.TeardownEnvironment(env.Id(), true/*force*/)
		if err != nil {
			env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
		}

		killedTasks, _, rlsErr := envs.taskman.KillTasks(envTasks.GetTaskIds())
		if rlsErr != nil {
			log.WithError(rlsErr).Warn("task teardown error")
		}
		log.WithFields(logrus.Fields{
			"killedCount": len(killedTasks),
			"lastEnvState": envState,
		}).
		Warn("environment deployment failed, tasks were cleaned up")
		return
	}

	env.subscribeToWfState(envs.taskman)
	defer env.unsubscribeFromWfState()

	// now we have the environment we should transition to start
	trans := NewStartActivityTransition(envs.taskman)
	if trans == nil {
		env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
		return
	}

	err = env.TryTransition(trans)
	if err != nil {
		env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
		return	
	}

	for {
		envState := env.CurrentState()
		switch envState { 
		case "CONFIGURED":
			// RUN finished so we can reset and delete the environment
			err := env.TryTransition(NewResetTransition(envs.taskman))
			if err != nil {
				env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
				return
			}
			err = envs.TeardownEnvironment(env.id, false)
			if err != nil {
				env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
				return
			}
			tasksForEnv := env.Workflow().GetTasks().GetTaskIds()
			_, _, err = envs.taskman.KillTasks(tasksForEnv)
			if err != nil {
				env.sendEnvironmentEvent(&event.EnvironmentEvent{EnvironmentID: env.Id().String(), Error: err})
				return
			}
			return
		case "ERROR":
			return
		case "MIXED":
			return
		case "":
			return
		}
	}
}
