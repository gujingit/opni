package management_test

import (
	"context"

	capabilityv1 "github.com/rancher/opni/pkg/apis/capability/v1"
	managementv1 "github.com/rancher/opni/pkg/apis/management/v1"
	"github.com/rancher/opni/pkg/capabilities"
	"github.com/rancher/opni/pkg/config/v1beta1"
	"github.com/rancher/opni/pkg/management"
	"github.com/rancher/opni/pkg/plugins"
	mock_capability "github.com/rancher/opni/pkg/test/mock/capability"
	"github.com/rancher/opni/pkg/test/testlog"
	"github.com/rancher/opni/pkg/util"
	"github.com/rancher/opni/pkg/util/waitctx"
	"google.golang.org/protobuf/types/known/emptypb"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type testCapabilityDataSource struct {
	store capabilities.BackendStore
}

func (t testCapabilityDataSource) CapabilitiesStore() capabilities.BackendStore {
	return t.store
}

func (t testCapabilityDataSource) NodeManagerServer() capabilityv1.NodeManagerServer {
	return capabilityv1.UnimplementedNodeManagerServer{}
}

var _ = Describe("Server", Ordered, Label("unit"), func() {
	var tv *testVars
	var capBackendStore capabilities.BackendStore
	BeforeAll(func() {
		capBackendStore = capabilities.NewBackendStore(capabilities.ServerInstallerTemplateSpec{}, testlog.Log)

		setupManagementServer(&tv, plugins.NoopLoader, management.WithCapabilitiesDataSource(testCapabilityDataSource{
			store: capBackendStore,
		}))()
	})
	It("should return valid cert info", func() {
		info, err := tv.client.CertsInfo(context.Background(), &emptypb.Empty{})
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Chain).To(HaveLen(1))
		Expect(info.Chain[0].Subject).To(Equal("CN=leaf"))
	})

	It("should handle configuration errors", func() {
		By("checking required config fields are set")
		conf := &v1beta1.ManagementSpec{
			HTTPListenAddress: "127.0.0.1:0",
		}
		ctx := waitctx.Background()
		server := management.NewServer(ctx, conf, tv.coreDataSource, plugins.NoopLoader)
		Expect(server.ListenAndServe(ctx).Error()).To(ContainSubstring("GRPCListenAddress not configured"))

		By("checking that invalid config fields cause errors")
		conf.GRPCListenAddress = "foo://bar"
		Expect(server.ListenAndServe(ctx)).To(MatchError(util.ErrUnsupportedProtocolScheme))
	})
	It("should allow querying capabilities from the data source", func() {
		list, err := tv.client.ListCapabilities(context.Background(), &emptypb.Empty{})
		Expect(err).NotTo(HaveOccurred())
		Expect(list.Items).To(BeEmpty())

		backend1 := mock_capability.NewTestCapabilityBackend(tv.ctrl, &mock_capability.CapabilityInfo{
			Name:              "capability1",
			CanInstall:        true,
			InstallerTemplate: "foo",
		})
		backend2 := mock_capability.NewTestCapabilityBackend(tv.ctrl, &mock_capability.CapabilityInfo{
			Name:              "capability2",
			CanInstall:        true,
			InstallerTemplate: "bar",
		})
		capBackendStore.Add("capability1", backend1)
		capBackendStore.Add("capability2", backend2)

		list, err = tv.client.ListCapabilities(context.Background(), &emptypb.Empty{})
		Expect(err).NotTo(HaveOccurred())
		Expect(list.Items).To(HaveLen(2))
		found := [2]bool{}
		for _, cap := range list.Names() {
			switch cap {
			case "capability1":
				found[0] = true
			case "capability2":
				found[1] = true
			default:
				Fail("unexpected capability name")
			}
		}

		cmd, err := tv.client.CapabilityInstaller(context.Background(), &managementv1.CapabilityInstallerRequest{
			Name: "capability1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(cmd.Command).To(Equal("foo"))

		cmd, err = tv.client.CapabilityInstaller(context.Background(), &managementv1.CapabilityInstallerRequest{
			Name: "capability2",
		})
		Expect(err).NotTo(HaveOccurred())
	})
})
