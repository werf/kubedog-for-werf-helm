package elimination

import (
	"context"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var configMapsGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

type recordingDynamicClient struct {
	dynamic.Interface
	rec *optionsRecorder
}

func (c *recordingDynamicClient) Resource(gvr schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &recordingNamespaceableResource{NamespaceableResourceInterface: c.Interface.Resource(gvr), rec: c.rec}
}

type recordingNamespaceableResource struct {
	dynamic.NamespaceableResourceInterface
	rec *optionsRecorder
}

func (r *recordingNamespaceableResource) Namespace(ns string) dynamic.ResourceInterface {
	return &recordingResource{ResourceInterface: r.NamespaceableResourceInterface.Namespace(ns), rec: r.rec}
}

type recordingResource struct {
	dynamic.ResourceInterface
	rec *optionsRecorder
}

func (r *recordingResource) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	r.rec.recordList(opts)
	return r.ResourceInterface.List(ctx, opts)
}

func (r *recordingResource) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	w, err := r.ResourceInterface.Watch(ctx, opts)
	r.rec.recordWatch(opts)
	return w, err
}

type optionsRecorder struct {
	mu        sync.Mutex
	lists     []metav1.ListOptions
	watches   []metav1.ListOptions
	watchedCh chan struct{}
	watchOnce sync.Once
}

func newOptionsRecorder() *optionsRecorder {
	return &optionsRecorder{watchedCh: make(chan struct{})}
}

func (r *optionsRecorder) recordList(opts metav1.ListOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lists = append(r.lists, opts)
}

func (r *optionsRecorder) recordWatch(opts metav1.ListOptions) {
	r.mu.Lock()
	r.watches = append(r.watches, opts)
	r.mu.Unlock()
	r.watchOnce.Do(func() { close(r.watchedCh) })
}

func (r *optionsRecorder) snapshot() ([]metav1.ListOptions, []metav1.ListOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]metav1.ListOptions(nil), r.lists...), append([]metav1.ListOptions(nil), r.watches...)
}

func newFakeClient(objects ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{
		configMapsGVR: "ConfigMapList",
	}, objects...)
}

func newConfigMap(namespace, name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("v1")
	u.SetKind("ConfigMap")
	u.SetNamespace(namespace)
	u.SetName(name)
	return u
}

// The reflector relies on the options it passes to List and Watch reaching the server, e.g.
// the WatchList mode of client-go hangs until timeout when they are replaced.
func TestTrackUntilEliminatedPassesReflectorOptionsToClient(t *testing.T) {
	const namespace, name = "test-ns", "test-cm"

	fakeClient := newFakeClient(newConfigMap(namespace, name))
	rec := newOptionsRecorder()
	client := &recordingDynamicClient{Interface: fakeClient, rec: rec}

	errCh := make(chan error, 1)
	go func() {
		errCh <- TrackUntilEliminated(context.Background(), client, []*EliminationTrackerSpec{{
			ResourceName:         name,
			Namespace:            namespace,
			GroupVersionResource: configMapsGVR,
		}}, EliminationTrackerOptions{Timeout: 10 * time.Second})
	}()

	select {
	case <-rec.watchedCh:
	case err := <-errCh:
		t.Fatalf("tracking finished before the resource was deleted: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("tracker did not start watching")
	}

	if err := fakeClient.Resource(configMapsGVR).Namespace(namespace).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete resource: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("TrackUntilEliminated: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TrackUntilEliminated did not return after the resource was deleted")
	}

	lists, watches := rec.snapshot()
	wantFieldSelector := "metadata.name=" + name

	if len(lists) == 0 {
		t.Fatal("List was not called")
	}
	if lists[0].FieldSelector != wantFieldSelector {
		t.Errorf("List FieldSelector = %q, want %q", lists[0].FieldSelector, wantFieldSelector)
	}
	if lists[0].ResourceVersion == "" {
		t.Error("List ResourceVersion set by the reflector was dropped")
	}

	if watches[0].FieldSelector != wantFieldSelector {
		t.Errorf("Watch FieldSelector = %q, want %q", watches[0].FieldSelector, wantFieldSelector)
	}
	if watches[0].TimeoutSeconds == nil {
		t.Error("Watch TimeoutSeconds set by the reflector was dropped")
	}
	if !watches[0].AllowWatchBookmarks {
		t.Error("Watch AllowWatchBookmarks set by the reflector was dropped")
	}
}

func TestTrackUntilEliminatedReturnsForAbsentResource(t *testing.T) {
	errCh := make(chan error, 1)
	go func() {
		errCh <- TrackUntilEliminated(context.Background(), newFakeClient(), []*EliminationTrackerSpec{{
			ResourceName:         "absent",
			Namespace:            "test-ns",
			GroupVersionResource: configMapsGVR,
		}}, EliminationTrackerOptions{Timeout: 10 * time.Second})
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("TrackUntilEliminated: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("TrackUntilEliminated did not return for an absent resource")
	}
}
