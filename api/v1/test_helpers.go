package v1

import (
	"os"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ptr "k8s.io/utils/pointer"
)

const (
	YtsaurusName    = "test-ytsaurus"
	CoreImageFirst  = "ytsaurus/ytsaurus-nightly:dev-864e613b6261e2458fbee05f1a440d4b277c9ee6"
	CoreImageSecond = "ytsaurus/ytsaurus-nightly:dev-4526e114f0d15afdd3b7448f65e49abdfb084de9"
)

func CreateBaseYtsaurusResource(namespace string) *Ytsaurus {
	masterVolumeSize, _ := resource.ParseQuantity("5Gi")
	execNodeVolumeSize, _ := resource.ParseQuantity("3Gi")
	execNodeCPU, _ := resource.ParseQuantity("1")
	execNodeMemory, _ := resource.ParseQuantity("2Gi")
	var httpNodePort int32
	if os.Getenv("YTOP_PROXY_PORT_K8S") != "" {
		port, err := strconv.Atoi(os.Getenv("YTOP_PROXY_PORT_K8S"))
		if err != nil {
			panic(err)
		}
		httpNodePort = int32(port)
	}

	return &Ytsaurus{
		ObjectMeta: metav1.ObjectMeta{
			Name:      YtsaurusName,
			Namespace: namespace,
		},
		Spec: YtsaurusSpec{
			EnableFullUpdate: true,
			IsManaged:        true,
			UseShortNames:    true,
			CoreImage:        CoreImageFirst,
			Discovery: DiscoverySpec{
				InstanceSpec: InstanceSpec{
					InstanceCount: 1,
				},
			},
			Bootstrap: &BootstrapSpec{
				TabletCellBundles: &BundlesBootstrapSpec{
					Sys: &BundleBootstrapSpec{
						TabletCellCount:        2,
						ChangelogPrimaryMedium: ptr.String("default"),
						SnapshotPrimaryMedium:  ptr.String("default"),
					},
					Default: &BundleBootstrapSpec{
						TabletCellCount:        2,
						ChangelogPrimaryMedium: ptr.String("default"),
						SnapshotPrimaryMedium:  ptr.String("default"),
					},
				},
			},
			PrimaryMasters: MastersSpec{
				CellTag: 1,
				InstanceSpec: InstanceSpec{
					InstanceCount: 1,
					Locations: []LocationSpec{
						{
							LocationType: "MasterChangelogs",
							Path:         "/yt/master-data/master-changelogs",
						},
						{
							LocationType: "MasterSnapshots",
							Path:         "/yt/master-data/master-snapshots",
						},
					},
					VolumeClaimTemplates: []EmbeddedPersistentVolumeClaim{
						{
							EmbeddedObjectMetadata: EmbeddedObjectMetadata{
								Name: "master-data",
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{"ReadWriteOnce"},
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: masterVolumeSize,
									},
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "master-data",
							MountPath: "/yt/master-data",
						},
					},
					Loggers: []TextLoggerSpec{
						{
							BaseLoggerSpec: BaseLoggerSpec{
								MinLogLevel: LogLevelDebug,
								Name:        "debug",
							},
							WriterType: LogWriterTypeFile,
						},
					},
				},
			},
			HTTPProxies: []HTTPProxiesSpec{
				{
					ServiceType: "NodePort",
					InstanceSpec: InstanceSpec{
						InstanceCount: 1,
					},
					HttpNodePort: &httpNodePort,
				},
			},
			DataNodes: []DataNodesSpec{
				{
					InstanceSpec: InstanceSpec{
						InstanceCount: 3,
						Locations: []LocationSpec{
							{
								LocationType: "ChunkStore",
								Path:         "/yt/node-data/chunk-store",
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "node-data",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{
										SizeLimit: &masterVolumeSize,
									},
								},
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "node-data",
								MountPath: "/yt/node-data",
							},
						},
					},
				},
			},
			TabletNodes: []TabletNodesSpec{
				{
					InstanceSpec: InstanceSpec{
						InstanceCount: 3,
						Loggers: []TextLoggerSpec{
							{
								BaseLoggerSpec: BaseLoggerSpec{
									MinLogLevel: LogLevelDebug,
									Name:        "debug",
								},
								WriterType: LogWriterTypeFile,
							},
						},
					},
				},
			},
			Schedulers: &SchedulersSpec{
				InstanceSpec: InstanceSpec{
					InstanceCount: 1,
				},
			},
			ControllerAgents: &ControllerAgentsSpec{
				InstanceSpec: InstanceSpec{
					InstanceCount: 1,
				},
			},
			ExecNodes: []ExecNodesSpec{
				{
					InstanceSpec: InstanceSpec{
						InstanceCount: 1,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    execNodeCPU,
								corev1.ResourceMemory: execNodeMemory,
							},
						},
						Loggers: []TextLoggerSpec{
							{
								BaseLoggerSpec: BaseLoggerSpec{
									MinLogLevel: LogLevelDebug,
									Name:        "debug",
								},
								WriterType: LogWriterTypeFile,
							},
						},
						Locations: []LocationSpec{
							{
								LocationType: "ChunkCache",
								Path:         "/yt/node-data/chunk-cache",
							},
							{
								LocationType: "Slots",
								Path:         "/yt/node-data/slots",
							},
						},
						Volumes: []corev1.Volume{
							{
								Name: "node-data",
								VolumeSource: corev1.VolumeSource{
									EmptyDir: &corev1.EmptyDirVolumeSource{
										SizeLimit: &execNodeVolumeSize,
									},
								},
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "node-data",
								MountPath: "/yt/node-data",
							},
						},
					},
				},
			},
		},
	}
}
