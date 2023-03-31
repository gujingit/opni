package backend

import (
	"context"
	capabilityv1 "github.com/rancher/opni/pkg/apis/capability/v1"
	opnicorev1 "github.com/rancher/opni/pkg/apis/core/v1"
	managementv1 "github.com/rancher/opni/pkg/apis/management/v1"
	"github.com/rancher/opni/pkg/capabilities/wellknown"
	"github.com/rancher/opni/pkg/management"
	"github.com/rancher/opni/pkg/storage"
	"github.com/rancher/opni/pkg/task"
	"github.com/rancher/opni/pkg/util"
	opnimeta "github.com/rancher/opni/pkg/util/meta"
	"github.com/rancher/opni/plugins/logging/pkg/apis/node"
	"github.com/rancher/opni/plugins/logging/pkg/gateway/drivers"
	"github.com/rancher/opni/plugins/logging/pkg/opensearchdata"
	loggingutil "github.com/rancher/opni/plugins/logging/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type LoggingBackend struct {
	capabilityv1.UnsafeBackendServer
	node.UnsafeNodeLoggingCapabilityServer
	LoggingBackendConfig
	util.Initializer

	nodeStatusMu      sync.RWMutex
	desiredNodeSpecMu sync.RWMutex
	watcher           *management.ManagementWatcherHooks[*managementv1.WatchEvent]
}

type LoggingBackendConfig struct {
	Logger              *zap.SugaredLogger             `validate:"required"`
	OpensearchCluster   *opnimeta.OpensearchClusterRef `validate:"required"`
	StorageBackend      storage.Backend                `validate:"required"`
	MgmtClient          managementv1.ManagementClient  `validate:"required"`
	NodeManagerClient   capabilityv1.NodeManagerClient `validate:"required"`
	UninstallController *task.Controller               `validate:"required"`
	ClusterDriver       drivers.ClusterDriver          `validate:"required"`
	OpensearchManager   *opensearchdata.Manager        `validate:"required"`
}

var _ node.NodeLoggingCapabilityServer = (*LoggingBackend)(nil)

// TODO: set up watches on underlying k8s objects to dynamically request a sync
func (b *LoggingBackend) Initialize(conf LoggingBackendConfig) {
	b.InitOnce(func() {
		if err := loggingutil.Validate.Struct(conf); err != nil {
			panic(err)
		}
		b.LoggingBackendConfig = conf

		b.watcher = management.NewManagementWatcherHooks[*managementv1.WatchEvent](context.TODO())
		b.watcher.RegisterHook(func(event *managementv1.WatchEvent) bool {
			return event.Type == managementv1.WatchEventType_Updated && slices.ContainsFunc(event.Cluster.Metadata.Capabilities, func(c *opnicorev1.ClusterCapability) bool {
				return c.Name == wellknown.CapabilityLogs
			})
		}, b.updateClusterMetadata)

		go func() {
			if err := b.waitForOpensearchClient(context.Background()); err != nil {
				b.Logger.With(zap.Error(err)).Error("could not reconcile opni agents with metadata index, some agents may not be included")
				return
			}

			clusters, err := b.MgmtClient.ListClusters(context.Background(), &managementv1.ListClustersRequest{})
			if err != nil {
				b.Logger.With(
					zap.Error(err),
				).Error("could not list clusters for reconciliation")
				return
			}

			if err := b.OpensearchManager.ReconcileClusterMetadata(context.Background(), clusters.Items); err != nil {
				b.Logger.With(zap.Error(err)).Error("could not reconcile opni agents with metadata index, some agents may not be included")
				return
			}

			b.watchClusterEvents(context.Background())
		}()
	})
}

func (b *LoggingBackend) Info(context.Context, *emptypb.Empty) (*capabilityv1.Details, error) {
	return &capabilityv1.Details{
		Name:    wellknown.CapabilityLogs,
		Source:  "plugin_logging",
		Drivers: drivers.ListClusterDrivers(),
	}, nil
}
