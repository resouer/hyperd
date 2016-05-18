package serverrpc

import (
	"github.com/golang/glog"
	"github.com/hyperhq/hyperd/types"
	"golang.org/x/net/context"
)

// PodStart starts Pod by podID, vmID
func (s *ServerRPC) PodStart(c context.Context, stream *types.PublicAPI_PodStartServer) error {
	glog.V(3).Infof("PodStart with request %v", req.String())

	// receive loop
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		stdin := ioutil.NopCloser(bytes.NewReader(req.Content))
		// TODO: stdout

	}

	code, cause, err := s.daemon.PodStart(stdin, stdout, req.PodId, req.VmId, req.Tag)
	if err != nil {
		return fmt.Errorf("PodStart error: %v", err)
	}

	return nil
}
