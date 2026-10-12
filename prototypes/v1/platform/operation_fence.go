package platform

import (
	"crypto/sha256"
	"sync"

	controller "peri-loom/controller"
)

var operationNamespaceFences [64]sync.Mutex

func lockOperationNamespace(namespace controller.Namespace) func() {
	digest := sha256.Sum256([]byte(namespaceKey(namespace)))
	fence := &operationNamespaceFences[int(digest[0])%len(operationNamespaceFences)]
	fence.Lock()
	return fence.Unlock
}
