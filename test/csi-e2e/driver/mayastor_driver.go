package driver

import (
	"fmt"
	"os"
	"strings"

	//	"github.com/kubernetes-csi/external-snapshotter/v2/pkg/apis/volumesnapshot/v1beta1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	//	"k8s.io/apimachinery/pkg/api/resource"
	//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// MayastorDriverNameVar is the environment variable use to switch the driver to be used.
const MayastorDriverNameVar = "MAYASTOR_CSI_DRIVER"

// MayastorDriver implements DynamicPVTestDriver interface
type MayastorDriver struct {
	driverName string
}

// InitMayastorDriver returns MayastorDriver that implements DynamicPVTestDriver interface
func InitMayastorDriver() PVTestDriver {
	driverName := os.Getenv(MayastorDriverNameVar)
	if driverName == "" {
		driverName = "io.openebs.csi-mayastor"
	}

	klog.Infof("Using Mayastor driver: %s", driverName)
	return &MayastorDriver{
		driverName: driverName,
	}
}

// normalizeProvisioner replaces any '/' character in the provisioner name to '-'.
// StorageClass name cannot contain '/' character.
func normalizeProvisioner(provisioner string) string {
	return strings.ReplaceAll(provisioner, "/", "-")
}

func (d *MayastorDriver) GetDynamicProvisionStorageClass(parameters map[string]string, mountOptions []string, reclaimPolicy *v1.PersistentVolumeReclaimPolicy, bindingMode *storagev1.VolumeBindingMode, allowedTopologyValues []string, namespace string) *storagev1.StorageClass {
	provisioner := d.driverName
	generateName := fmt.Sprintf("%s-%s-dynamic-sc-", namespace, normalizeProvisioner(provisioner))
	return getStorageClass(generateName, provisioner, parameters, mountOptions, reclaimPolicy, bindingMode, nil)
}

/*
func (d *MayastorDriver) GetVolumeSnapshotClass(namespace string) *v1beta1.VolumeSnapshotClass {
	provisioner := d.driverName
	generateName := fmt.Sprintf("%s-%s-dynamic-sc-", namespace, normalizeProvisioner(provisioner))
	return getVolumeSnapshotClass(generateName, provisioner)
}
*/

func GetParameters() map[string]string {
	return map[string]string{
		"skuName": "Standard_LRS",
	}
}
