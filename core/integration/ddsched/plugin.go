/*
 * === This file is part of ALICE O² ===
 *
 * Copyright 2021 CERN and copyright holders of ALICE O².
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

//go:generate protoc --go_out=. --go-grpc_out=require_unimplemented_servers=false:. protos/ddsched.proto

package ddsched

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	ddpb "github.com/AliceO2Group/Control/core/integration/ddsched/protos"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const DCS_CALL_TIMEOUT = 10 * time.Second

type Plugin struct {
	dcsHost        string
	dcsPort        int

	ddSchedClient *RpcClient
}

func NewPlugin(endpoint string) *Plugin {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.WithField("endpoint", endpoint).
			WithError(err).
			Error("bad service endpoint")
		return nil
	}

	portNumber, _ := strconv.Atoi(u.Port())

	return &Plugin{
		dcsHost:       u.Hostname(),
		dcsPort:       portNumber,
		ddSchedClient: nil,
	}
}

func (p *Plugin) GetName() string {
	return "ddsched"
}

func (p *Plugin) Init(_ string) error {
	if p.ddSchedClient == nil {
		callTimeout := DCS_CALL_TIMEOUT
		cxt, cancel := context.WithTimeout(context.Background(), callTimeout)
		p.ddSchedClient = NewClient(cxt, cancel, viper.GetString("ddSchedulerEndpoint"))
		if p.ddSchedClient == nil {
			return fmt.Errorf("failed to connect to DD scheduler service on %s", viper.GetString("ddSchedulerEndpoint"))
		}
	}
	return nil
}

func (p *Plugin) ObjectStack(varStack map[string]string) (stack map[string]interface{}) {
	stack = make(map[string]interface{})
	stack["PartitionInitialize"] = func() (out string) {	// must formally return string even when we return nothing
		log.Debug("performing DD scheduler PartitionInitialize")

		var (
			stfbHostIdMapS, stfsHostIdMapS string
			ok bool
		)

		stfbHostIdMapS, ok = varStack["stfb_host_id_map"]
		if !ok {
			log.Debug("no DD stfb_host_id_map set")
			stfbHostIdMapS = "{}"
		}
		stfbHostIdMap := make(map[string]string)
		stfbBytes := []byte(stfbHostIdMapS)
		err := json.Unmarshal(stfbBytes, &stfbHostIdMap)
		if err != nil {
			log.WithError(err).Error("error processing DD stfb_host_id_map")
			return
		}

		stfsHostIdMapS, ok = varStack["stfs_host_id_map"]
		if !ok {
			log.Debug("no DD stfs_host_id_map set")
			stfsHostIdMapS = "{}"
		}
		stfsHostIdMap := make(map[string]string)
		stfsBytes := []byte(stfsHostIdMapS)
		err = json.Unmarshal(stfsBytes, &stfsHostIdMap)
		if err != nil {
			log.WithError(err).Error("error processing DD stfs_host_id_map")
			return
		}

		envId, ok := varStack["environment_id"]
		if !ok {
			log.Error("cannot acquire environment ID for DD scheduler PartitionInitialize")
			return
		}

		in := ddpb.PartitionInitRequest{
			PartitionInfo: &ddpb.PartitionInfo{
				EnvironmentId: envId,
				PartitionId:   envId,
			},
			StfbHostIdMap: stfbHostIdMap,
			StfsHostIdMap: stfsHostIdMap,
		}
		if p.ddSchedClient == nil {
			log.WithError(fmt.Errorf("DD scheduler plugin not initialized")).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionInitialize")
			return
		}
		if p.ddSchedClient.GetConnState() != connectivity.Ready {
			log.WithError(fmt.Errorf("DD scheduler client connection not available")).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionInitialize")
			return
		}

		var response *ddpb.PartitionResponse
		response, err = p.ddSchedClient.PartitionInitialize(context.Background(), &in, grpc.EmptyCallOption{})
		if err != nil {
			log.WithError(err).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionInitialize")
			return
		}
		if response.PartitionState != ddpb.PartitionState_PARTITION_CONFIGURING {
			log.WithError(fmt.Errorf("PartitionInitialize returned unexpected state %s (expected: PARTITION_CONFIGURING)", response.PartitionState.String())).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionInitialize")
		}
		return
	}
	stack["PartitionTerminate"] = func() (out string) {	// must formally return string even when we return nothing
		log.Debug("performing DD scheduler PartitionTerminate")

		envId, ok := varStack["environment_id"]
		if !ok {
			log.Error("cannot acquire environment ID for DD scheduler PartitionTerminate")
			return
		}

		in := ddpb.PartitionTermRequest{
			PartitionInfo: &ddpb.PartitionInfo{
				EnvironmentId: envId,
				PartitionId:   envId,
			},
		}
		if p.ddSchedClient == nil {
			log.WithError(fmt.Errorf("DD scheduler plugin not initialized")).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionTerminate")
			return
		}
		if p.ddSchedClient.GetConnState() != connectivity.Ready {
			log.WithError(fmt.Errorf("DD scheduler client connection not available")).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionTerminate")
			return
		}

		var (
			response *ddpb.PartitionResponse
			err error
		)
		response, err = p.ddSchedClient.PartitionTerminate(context.Background(), &in, grpc.EmptyCallOption{})
		if err != nil {
			log.WithError(err).
				WithField("endpoint", viper.GetString("dcsServiceEndpoint")).
				Error("failed to perform DD scheduler PartitionTerminate")
		}
		if response.PartitionState != ddpb.PartitionState_PARTITION_TERMINATING {
			log.WithError(fmt.Errorf("PartitionInitialize returned unexpected state %s (expected: PARTITION_TERMINATING)", response.PartitionState.String())).
				WithField("endpoint", viper.GetString("ddSchedulerEndpoint")).
				Error("failed to perform DD scheduler PartitionTerminate")
		}
		return
	}

	return
}

func (p *Plugin) Destroy() error {
	return p.ddSchedClient.Close()
}

