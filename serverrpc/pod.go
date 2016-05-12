package serverrpc

import (
	"os"

	"github.com/golang/glog"
	"github.com/hyperhq/hyperd/types"
	"golang.org/x/net/context"
)

func (s *ServerRPC) PodLabels(c context.Context, req *types.PodLabelsRequest) (*types.PodLabelsResponse, error) {
	glog.V(3).Infof("Set pod labels with request %v", req.String())

	err := s.daemon.SetPodLabels(req.PodID, req.override, req.labels)
	if err != nil {
		glog.Errorf("PodLabels error: %v", err)
		return nil, err
	}

	return &types.PodLabelsResponse{}, nil
}
