package components

import (
	"context"
	ytv1 "github.com/ytsaurus/yt-k8s-operator/api/v1"
	"github.com/ytsaurus/yt-k8s-operator/pkg/apiproxy"
	"github.com/ytsaurus/yt-k8s-operator/pkg/consts"
	"github.com/ytsaurus/yt-k8s-operator/pkg/labeller"
	"github.com/ytsaurus/yt-k8s-operator/pkg/resources"
	"github.com/ytsaurus/yt-k8s-operator/pkg/ytconfig"
)

type dataNode struct {
	ServerComponentBase
	master Component
}

func NewDataNode(
	cfgen *ytconfig.Generator,
	ytsaurus *apiproxy.Ytsaurus,
	master Component,
	spec ytv1.DataNodesSpec,
) Component {
	resource := ytsaurus.GetResource()
	l := labeller.Labeller{
		ObjectMeta:     &resource.ObjectMeta,
		APIProxy:       ytsaurus.APIProxy(),
		ComponentLabel: cfgen.FormatComponentStringWithDefault(consts.YTComponentLabelDataNode, spec.Name),
		ComponentName:  cfgen.FormatComponentStringWithDefault("DataNode", spec.Name),
		MonitoringPort: consts.NodeMonitoringPort,
	}

	server := NewServer(
		&l,
		ytsaurus,
		&spec.InstanceSpec,
		"/usr/bin/ytserver-node",
		"ytserver-data-node.yson",
		cfgen.GetDataNodesStatefulSetName(spec.Name),
		cfgen.GetDataNodesServiceName(spec.Name),
		func() ([]byte, error) {
			return cfgen.GetDataNodeConfig(spec)
		},
		func(data []byte) (bool, error) {
			return cfgen.NeedDataNodeConfigReload(spec, data)
		},
	)

	return &dataNode{
		ServerComponentBase: ServerComponentBase{
			ComponentBase: ComponentBase{
				labeller: &l,
				ytsaurus: ytsaurus,
				cfgen:    cfgen,
			},
			server: server,
		},
		master: master,
	}
}

func (n *dataNode) Fetch(ctx context.Context) error {
	return resources.Fetch(ctx, []resources.Fetchable{
		n.server,
	})
}

func (n *dataNode) doSync(ctx context.Context, dry bool) (SyncStatus, error) {
	var err error

	if n.ytsaurus.GetClusterState() == ytv1.ClusterStateRunning && n.server.NeedUpdate() {
		return SyncStatusNeedFullUpdate, err
	}

	if n.ytsaurus.GetClusterState() == ytv1.ClusterStateUpdating {
		if n.ytsaurus.GetUpdateState() == ytv1.UpdateStateWaitingForPodsRemoval {
			updatingComponents := n.ytsaurus.GetLocalUpdatingComponents()
			if updatingComponents == nil {
				return SyncStatusUpdating, n.removePods(ctx, dry)
			}
		}
	}

	if !(n.master.Status(ctx) == SyncStatusReady) {
		return SyncStatusBlocked, err
	}

	if n.server.NeedSync() {
		if !dry {
			err = n.server.Sync(ctx)
		}
		return SyncStatusPending, err
	}

	if !n.server.ArePodsReady(ctx) {
		return SyncStatusBlocked, err
	}

	return SyncStatusReady, err
}

func (n *dataNode) Status(ctx context.Context) SyncStatus {
	status, err := n.doSync(ctx, true)
	if err != nil {
		panic(err)
	}

	return status
}

func (n *dataNode) Sync(ctx context.Context) error {
	_, err := n.doSync(ctx, false)
	return err
}