package management

import (
	context "context"
	"time"

	core "github.com/kralicky/opni-monitoring/pkg/core"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func (m *Server) ListClusters(
	ctx context.Context,
	in *ListClustersRequest,
) (*core.ClusterList, error) {
	clusterList, err := m.clusterStore.ListClusters(ctx, in.MatchLabels)
	if err != nil {
		return nil, grpcError(err)
	}
	return clusterList, nil
}

func (m *Server) DeleteCluster(
	ctx context.Context,
	ref *core.Reference,
) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, grpcError(m.clusterStore.DeleteCluster(ctx, ref))
}

func (m *Server) GetCluster(
	ctx context.Context,
	ref *core.Reference,
) (*core.Cluster, error) {
	if cluster, err := m.clusterStore.GetCluster(ctx, ref); err != nil {
		return nil, grpcError(err)
	} else {
		return cluster, nil
	}
}

func (m *Server) WatchClusters(
	in *WatchClustersRequest,
	stream Management_WatchClustersServer,
) error {
	known := map[string]*core.Reference{}
	for _, cluster := range in.KnownClusters.Items {
		known[cluster.Name] = cluster
	}
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			clusters, err := m.clusterStore.ListClusters(context.Background(), nil)
			updatedIds := map[string]struct{}{}
			if err != nil {
				return grpcError(err)
			}
			for _, cluster := range clusters.Items {
				updatedIds[cluster.Id] = struct{}{}
				if _, ok := known[cluster.Id]; !ok {
					ref := cluster.Reference()
					known[cluster.Id] = ref
					if err := stream.Send(&WatchEvent{
						Cluster: ref,
						Type:    WatchEventType_Added,
					}); err != nil {
						return status.Error(codes.Internal, err.Error())
					}
				}
			}
			for id, cluster := range known {
				if _, ok := updatedIds[id]; !ok {
					delete(known, id)
					if err := stream.Send(&WatchEvent{
						Cluster: cluster,
						Type:    WatchEventType_Deleted,
					}); err != nil {
						return status.Error(codes.Internal, err.Error())
					}
				}
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (m *Server) EditCluster(
	ctx context.Context,
	in *EditClusterRequest,
) (*core.Cluster, error) {
	storedCluster, err := m.clusterStore.GetCluster(ctx, in.Cluster)
	if err != nil {
		return nil, grpcError(err)
	}
	storedCluster.Labels = in.Labels
	if c, err := m.clusterStore.UpdateCluster(ctx, storedCluster); err != nil {
		return nil, grpcError(err)
	} else {
		return c, nil
	}
}
