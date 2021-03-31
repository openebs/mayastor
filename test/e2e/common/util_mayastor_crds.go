package common

// Utility functions for Mayastor CRDs
import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strings"

	. "github.com/onsi/gomega"

	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Status part of the mayastor volume CRD
type MayastorVolStatus struct {
	State    string
	Node     string
	Replicas []string
}

func GetMSV(uuid string) *MayastorVolStatus {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
	msv, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).Get(context.TODO(), uuid, metav1.GetOptions{})
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if msv == nil {
		return nil
	}
	status, found, err := unstructured.NestedFieldCopy(msv.Object, "status")
	if err != nil {
		fmt.Println(err)
		return nil
	}

	if !found {
		return nil
	}
	msVol := MayastorVolStatus{}

	msVol.Replicas = make([]string, 0, 4)

	v := reflect.ValueOf(status)
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			sKey := key.Interface().(string)
			val := v.MapIndex(key)
			switch sKey {
			case "state":
				msVol.State = val.Interface().(string)
			case "nexus":
				nexusInt := val.Interface().(map[string]interface{})
				if node, ok := nexusInt["node"].(string); ok {
					msVol.Node = node
				}
			case "replicas":
				replicas := val.Interface().([]interface{})
				for _, replica := range replicas {
					replicaMap := reflect.ValueOf(replica)
					if replicaMap.Kind() == reflect.Map {
						for _, field := range replicaMap.MapKeys() {
							switch field.Interface().(string) {
							case "node":
								value := replicaMap.MapIndex(field)
								msVol.Replicas = append(msVol.Replicas, value.Interface().(string))
							}
						}
					}
				}
			}
		}
		// Note: msVol.Node can be unassigned here if the volume is not mounted
		Expect(msVol.State).NotTo(Equal(""))
		Expect(len(msVol.Replicas)).To(BeNumerically(">", 0))
		return &msVol
	}
	return nil
}

// Check for a deleted Mayastor Volume,
// the object does not exist if deleted
func IsMSVDeleted(uuid string) bool {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msv, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).Get(context.TODO(), uuid, metav1.GetOptions{})

	if err != nil {
		// Unfortunately there is no associated error code so we resort to string comparison
		if strings.HasPrefix(err.Error(), "mayastorvolumes.openebs.io") &&
			strings.HasSuffix(err.Error(), " not found") {
			return true
		}
	}

	Expect(err).To(BeNil())
	Expect(msv).ToNot(BeNil())
	return false
}

func DeleteMSV(uuid string) error {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).Delete(context.TODO(), uuid, metav1.DeleteOptions{})
	return err
}

// Retrieve the state of a Mayastor Volume
func GetMsvState(uuid string) string {
	msv := GetMSV(uuid)
	Expect(msv).ToNot(BeNil())
	return msv.State
}

// Retrieve the nexus node hosting the Mayastor Volume,
// and the names of the replica nodes
func GetMsvNodes(uuid string) (string, []string) {
	msv := GetMSV(uuid)
	Expect(msv).ToNot(BeNil())
	return msv.Node, msv.Replicas
}

// Return a group version resource for a MSV
func getMsvGvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}
}

// Get the k8s MSV CRD
func getMsv(uuid string) (*unstructured.Unstructured, error) {
	msvGVR := getMsvGvr()
	return gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).Get(context.TODO(), uuid, metav1.GetOptions{})
}

func retrieveFieldValue(uns *unstructured.Unstructured, fields ...string) (interface{}, error) {
	field, found, err := unstructured.NestedFieldCopy(uns.Object, fields...)
	if err != nil {
		// The last field is the one that we were looking for.
		lastFieldIndex := len(fields) - 1
		return nil, fmt.Errorf("failed to get field %s with error %v", fields[lastFieldIndex], err)
	}
	if !found {
		// The last field is the one that we were looking for.
		lastFieldIndex := len(fields) - 1
		return nil, fmt.Errorf("failed to find field %s", fields[lastFieldIndex])
	}
	return field, nil
}

func retrieveFieldStringValue(uns *unstructured.Unstructured, fields ...string) (string, error) {
	repl, err := retrieveFieldValue(uns, fields...)
	if err == nil {
		return reflect.ValueOf(repl).Interface().(string), nil
	}
	return "?", err
}

// Get a field within the MSV.
// The "fields" argument specifies the path within the MSV where the field should be found.
// E.g. for the replicaCount field which is nested under the MSV spec the function should be called like:
//		getMsvFieldValue(<uuid>, "spec", "replicaCount")
func getMsvFieldValue(uuid string, fields ...string) (interface{}, error) {
	msv, err := getMsv(uuid)
	if err != nil {
		return nil, fmt.Errorf("Failed to get MSV with error %v", err)
	}
	if msv == nil {
		return nil, fmt.Errorf("MSV with uuid %s does not exist", uuid)
	}

	return retrieveFieldValue(msv, fields...)
}

// GetNumReplicas returns the number of replicas in the MSV.
// An error is returned if the number of replicas cannot be retrieved.
func GetNumReplicas(uuid string) (int64, error) {
	// Get the number of replicas from the MSV.
	repl, err := getMsvFieldValue(uuid, "spec", "replicaCount")
	if err != nil {
		return 0, err
	}
	if repl == nil {
		return 0, fmt.Errorf("Failed to get replicaCount")
	}

	return reflect.ValueOf(repl).Interface().(int64), nil
}

// UpdateNumReplicas sets the number of replicas in the MSV to the desired number.
// An error is returned if the number of replicas cannot be updated.
func UpdateNumReplicas(uuid string, numReplicas int64) error {
	msv, err := getMsv(uuid)
	if err != nil {
		return fmt.Errorf("Failed to get MSV with error %v", err)
	}
	if msv == nil {
		return fmt.Errorf("MSV not found")
	}

	// Set the number of replicas in the MSV.
	err = unstructured.SetNestedField(msv.Object, numReplicas, "spec", "replicaCount")
	if err != nil {
		return err
	}

	// Update the k8s MSV object.
	msvGVR := getMsvGvr()
	_, err = gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).Update(context.TODO(), msv, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("Failed to update MSV: %v", err)
	}
	return nil
}

// GetNumChildren returns the number of nexus children listed in the MSV
func GetNumChildren(uuid string) int {
	children, err := getMsvFieldValue(uuid, "status", "nexus", "children")
	if err != nil {
		return 0
	}
	if children == nil {
		return 0
	}

	switch reflect.TypeOf(children).Kind() {
	case reflect.Slice:
		return reflect.ValueOf(children).Len()
	}
	return 0
}

// NexusChild represents the information stored in the MSV about the child
type NexusChild struct {
	State string
	URI   string
}

// GetChildren returns a slice containing information about the children.
// An error is returned if the child information cannot be retrieved.
func GetChildren(uuid string) ([]NexusChild, error) {
	children, err := getMsvFieldValue(uuid, "status", "nexus", "children")
	if err != nil {
		return nil, fmt.Errorf("Failed to get children with error %v", err)
	}
	if children == nil {
		return nil, fmt.Errorf("Failed to find children")
	}

	nexusChildren := make([]NexusChild, 2)

	switch reflect.TypeOf(children).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(children)
		for i := 0; i < s.Len(); i++ {
			child := s.Index(i).Elem()
			if child.Kind() == reflect.Map {
				for _, key := range child.MapKeys() {
					skey := key.Interface().(string)
					switch skey {
					case "state":
						nexusChildren[i].State = child.MapIndex(key).Interface().(string)
					case "uri":
						nexusChildren[i].URI = child.MapIndex(key).Interface().(string)
					}
				}
			}
		}
	}

	return nexusChildren, nil
}

// GetNexusState returns the nexus state from the MSV.
// An error is returned if the nexus state cannot be retrieved.
func GetNexusState(uuid string) (string, error) {
	// Get the state of the nexus from the MSV.
	state, err := getMsvFieldValue(uuid, "status", "nexus", "state")
	if err != nil {
		return "", err
	}
	if state == nil {
		return "", fmt.Errorf("Failed to get nexus state")
	}

	return reflect.ValueOf(state).Interface().(string), nil
}

// IsVolumePublished returns true if the volume is published.
// A volume is published if the "targetNodes" field exists in the MSV.
func IsVolumePublished(uuid string) bool {
	_, err := getMsvFieldValue(uuid, "status", "targetNodes")
	return err == nil
}

func CheckForMSVs() (bool, error) {
	logf.Log.Info("CheckForMSVs")
	foundResources := false

	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err == nil && msvs != nil && len(msvs.Items) != 0 {
		logf.Log.Info("CheckForVolumeResources: found MayastorVolumes",
			"MayastorVolumes", msvs.Items)
		foundResources = true
	}
	return foundResources, err
}

func CheckAllMsvsAreHealthy() error {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorvolumes",
	}

	allHealthy := true
	retrieveErrors := false
	msvs, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err == nil && msvs != nil && len(msvs.Items) != 0 {
		for _, msv := range msvs.Items {
			msvName, _ := retrieveFieldStringValue(&msv, "metadata", "name")
			state, err := retrieveFieldStringValue(&msv, "status", "state")
			if err == nil {
				if state != "healthy" {
					logf.Log.Info("CheckAllMsvsAreHealthy", "msvName", msvName, "state", state)
					allHealthy = false
				}
			} else {
				logf.Log.Info("CheckAllMsvsAreHealthy failed to access status.state", "msvName", msvName)
				retrieveErrors = true
			}
		}
	}

	if retrieveErrors {
		return fmt.Errorf("error accessing MSV status.state")
	}
	if !allHealthy {
		return fmt.Errorf("all MSVs were not healthy")
	}
	return err
}

func CheckAllPoolsAreOnline() error {
	msvGVR := schema.GroupVersionResource{
		Group:    "openebs.io",
		Version:  "v1alpha1",
		Resource: "mayastorpools",
	}

	allHealthy := true
	retrieveErrors := false
	pools, err := gTestEnv.DynamicClient.Resource(msvGVR).Namespace(NSMayastor).List(context.TODO(), metav1.ListOptions{})
	if err == nil && pools != nil && len(pools.Items) != 0 {
		for _, pool := range pools.Items {
			poolName, _ := retrieveFieldStringValue(&pool, "metadata", "name")
			state, err := retrieveFieldStringValue(&pool, "status", "state")
			if err == nil {
				if state != "online" {
					logf.Log.Info("CheckAllPoolsAreOnline", "pool", poolName, "state", state)
					allHealthy = false
				}
			} else {
				logf.Log.Info("CheckAllPoolsAreOnline failed to access status.state", "pool", poolName, "error", err)
				retrieveErrors = true
			}
		}
	}

	if retrieveErrors {
		return fmt.Errorf("error accessing pools status.state")
	}
	if !allHealthy {
		return fmt.Errorf("all pools were not healthy")
	}
	return err
}
