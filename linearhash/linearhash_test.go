package linearhash

import (
	"fmt"
	"goshawkdb.io/client"
	"goshawkdb.io/tests/harness"
	"math/rand"
	"testing"
	"time"
)

func createEmpty(th *harness.TestHelper, txr client.Transactor) *LHash {
	lh, err := NewEmptyLHash(txr)
	if err != nil {
		th.Fatal(err)
		return nil
	}
	return lh
}

func assertSize(th *harness.TestHelper, txr client.Transactor, lh *LHash, expected int64) {
	size, err := lh.Size(txr)
	if err != nil {
		th.Fatal(err)
	} else if size != expected {
		th.Fatal(fmt.Sprintf("Expected to have %v size. Got %v", expected, size))
	}
}

func TestCreateNew(t *testing.T) {
	th := harness.NewTestHelper(t)
	defer th.Shutdown()

	c := th.CreateConnections(1)[0]

	lh1 := createEmpty(th, c)
	assertSize(th, c, lh1, 0)

	lh2 := LHashFromObj(lh1.ObjRef)
	assertSize(th, c, lh2, 0)
}

func TestPutGetForEach(t *testing.T) {
	th := harness.NewTestHelper(t)
	defer th.Shutdown()

	c0 := th.CreateConnections(1)[0]

	lh := createEmpty(th, c0)

	objCount := int64(1024)
	// create objs
	res, err := c0.Transact(func(txn *client.Transaction) (interface{}, error) {
		result := make(map[string]client.RefCap)
		for idx := int64(0); idx < objCount; idx++ {
			str := fmt.Sprintf("%v", idx)
			if objRef, err := txn.Create(([]byte)(str)); err != nil || txn.RestartNeeded() {
				return nil, err
			} else {
				result[str] = objRef
			}
		}
		return result, nil
	})
	if err != nil {
		th.Fatal(err)
	}

	objs := res.(map[string]client.RefCap)
	start := time.Now()
	_, err = c0.Transact(func(txn *client.Transaction) (interface{}, error) {
		for str, objRef := range objs {
			th.Log("msg", "Putting", "key", str, "value", objRef)
			if err := lh.Put(txn, ([]byte)(str), objRef); err != nil || txn.RestartNeeded() {
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		th.Fatal(err)
	}
	assertSize(th, c0, lh, objCount)

	mid := time.Now()
	for str, objRef := range objs {
		if objRefFound, err := lh.Find(c0, ([]byte)(str)); err != nil {
			th.Fatal(err)
		} else if objRefFound == nil {
			th.Fatal(fmt.Sprintf("Failed to find entry for %v", str))
		} else if !objRefFound.SameReferent(objRef) {
			th.Fatal(fmt.Sprintf("Entry for %v has value in %v instead of %v", str, objRefFound, objRef))
		}
	}
	assertSize(th, c0, lh, objCount)
	end := time.Now()

	_, err = c0.Transact(func(txn *client.Transaction) (interface{}, error) {
		th.Log("msg", "foreach restart")
		objsItr := make(map[string]bool, len(objs))
		err := lh.ForEach(txn, func(key []byte, objRef client.RefCap) error {
			str := string(key)
			th.Log("msg", "yielded", "key", str)
			if objsItr[str] {
				return fmt.Errorf("ForEach yielded key %v twice!", str)
			}
			objsItr[str] = true
			ref, found := objs[str]
			if !found {
				return fmt.Errorf("ForEach yielded unknown key: %v", str)
			}
			if !objRef.SameReferent(ref) {
				return fmt.Errorf("ForEach yielded unexpected value for key: %v (expected %v; actual %v)", str, ref, objRef)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if len(objsItr) != len(objs) {
			return nil, fmt.Errorf("ForEach yielded incorrect number of values: %v vs %v", len(objsItr), len(objs))
		}
		return nil, nil
	})
	if err != nil {
		th.Fatal(err)
	}
	forEachEnd := time.Now()

	th.Log("inserting", mid.Sub(start), "fetching", end.Sub(mid), "forEach", forEachEnd.Sub(end))
}

func TestSoak(t *testing.T) {
	// Sadly undirected, but nevertheless fairly sensible way of doing
	// testing.
	th := harness.NewTestHelper(t)
	defer th.Shutdown()

	c := th.CreateConnections(1)[0]

	lh := createEmpty(th, c)

	seed := time.Now().UnixNano()
	// seed = int64(1475936141644630799)
	th.Log("Seed", seed)
	rng := rand.New(rand.NewSource(seed))
	// we use contents to mirror the state of the LHash
	contents := make(map[string]string)

	var err error
	for i := 16384; i > 0; i-- {
		lenContents := len(contents)
		// we bias creation of new keys by 999 with 1 more for reset
		op := rng.Intn((3*lenContents)+1000) - 1000
		opClass := 0
		opArg := 0
		if lenContents > 0 {
			opClass = op / lenContents
			opArg = op % lenContents
		}
		switch {
		case op == -1: // reset
			lh, err = NewEmptyLHash(c)
			if err != nil {
				th.Fatal(err)
				return
			}
			contents = make(map[string]string)
			th.Log("action", "NewLHash")

		case op < -1: // add new key
			key := fmt.Sprintf("%v", lenContents)
			value := fmt.Sprintf("Hello%v-%v", i, key)
			_, err = c.Transact(func(txn *client.Transaction) (interface{}, error) {
				if valueObj, err := txn.Create([]byte(value)); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, lh.Put(txn, []byte(key), valueObj)
				}
			})
			if err != nil {
				th.Fatal(err)
			}
			contents[key] = value
			th.Log("action", "Put", "key", key, "value", value)

		case opClass == 0: // find key
			key := fmt.Sprintf("%v", opArg)
			value := contents[key]
			inContents := len(value) != 0
			th.Log("action", "Find", "key", key, "value", value, "inContents", inContents)
			result, err := c.Transact(func(txn *client.Transaction) (interface{}, error) {
				if valueObj, err := lh.Find(txn, []byte(key)); err != nil || txn.RestartNeeded() {
					return nil, err
				} else if valueObj == nil {
					return nil, nil
				} else if val, _, err := txn.Read(*valueObj); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return val, nil
				}
			})
			if err != nil {
				th.Fatal(err)
			}
			if s, ok := result.([]byte); inContents && (result == nil || !ok || string(s) != value) {
				th.Fatal(fmt.Sprintf("%v Failed to retrieve string value: %v", key, result))
			} else if !inContents && result != nil {
				th.Fatal(fmt.Sprintf("Got result even after remove: %v", result))
			}

		case opClass == 1: // remove key
			key := fmt.Sprintf("%v", opArg)
			inContents := len(contents[key]) != 0
			th.Log("action", "Remove", "key", key, "inContents", inContents)
			if err = lh.Remove(c, []byte(key)); err != nil {
				th.Fatal(err)
			} else if inContents {
				contents[key] = ""
			}

		case opClass == 2: // re-put existing key
			key := fmt.Sprintf("%v", opArg)
			value := contents[key]
			inContents := len(value) != 0
			th.Log("action", "Reput", "key", key, "value", value, "inContents", inContents)
			if !inContents {
				value = fmt.Sprintf("Hello%v-%v", i, key)
				contents[key] = value
			}
			_, err = c.Transact(func(txn *client.Transaction) (interface{}, error) {
				if valueObj, err := txn.Create([]byte(value)); err != nil || txn.RestartNeeded() {
					return nil, err
				} else {
					return nil, lh.Put(txn, []byte(key), valueObj)
				}
			})
			if err != nil {
				th.Fatal(err)
			}

		default:
			th.Fatal(fmt.Sprintf("Unexpected op %v (class: %v; arg %v)", op, opClass, opArg))
		}
	}
}
