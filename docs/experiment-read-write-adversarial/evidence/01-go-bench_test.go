package platform

import (
 "bytes"
 "encoding/json"
 "fmt"
 "os"
 "testing"
 "strings"
)

// Exact frozen writeBatchAt body, changing only capacity calculation.
func (journal *operationJournal) adversarialExactWriteBatchAt(batch []*operationCommit, offset int64) error {
 if journal.testWriteGate != nil { journal.testWriteGate() }
 if err := journal.verifyPathIdentity(); err != nil { return ErrStateUncertain }
 size := 0
 for _, commit := range batch { size += len(commit.line) }
 run := make([]byte, 0, size)
 for _, commit := range batch { run = append(run, commit.line...) }
 if _, err := journal.file.WriteAt(run, offset); err != nil { return ErrStateUncertain }
 if err := journal.file.Sync(); err != nil { return ErrStateUncertain }
 return journal.writeWitnessFor(batch[len(batch)-1])
}

var adversarialSink []byte

func adversarialBatch(n int) ([]*operationCommit, []byte) {
 batch := make([]*operationCommit, n)
 var expected []byte
 previous := ""
 for i := range batch {
  mutation := journalMutation{Version: operationJournalVersion, Revision:uint64(i+1), Previous:previous}
  if os.Getenv("RW01_FULL_ONLY")=="1" { mutation.Remove=[]string{strings.Repeat("x",32500)} }
  payload, _ := json.Marshal(mutation)
  hash := digestHex(payload)
  line, _ := json.Marshal(journalEntry{Payload:mutation, Hash:hash})
  batch[i] = &operationCommit{revision:uint64(i+1), payload:mutation, hash:hash, line:append(line,'\n')}
  previous=hash
  expected=append(expected,batch[i].line...)
 }
 return batch, expected
}

func BenchmarkAdversarial(b *testing.B) {
 arms:=[]string{"frozen", "exact"}
 if os.Getenv("RW01_ORDER")=="reverse" { arms=[]string{"exact","frozen"} }
 counts:=[]int{1,32}
 if os.Getenv("RW01_FULL_ONLY")=="1" {counts=[]int{32}}
 for _, n:=range counts {
  batch,expected:=adversarialBatch(n)
  for _, mode:=range []string{"actual_sync", "buffer_model"} {
   for _,arm:=range arms {
    b.Run(fmt.Sprintf("%s/batch%d/%s",mode,n,arm),func(b *testing.B){
     b.ReportAllocs()
     b.SetBytes(int64(len(expected)))
     if mode=="buffer_model" {
      b.ResetTimer()
      for i:=0;i<b.N;i++ {
       size:=operationJournalBatchBytes
       if arm=="exact" { size=0;for _,c:=range batch {size+=len(c.line)} }
       run:=make([]byte,0,size)
       for _,c:=range batch {run=append(run,c.line...)}
       adversarialSink=run
      }
      b.StopTimer()
      if !bytes.Equal(adversarialSink,expected) { b.Fatal("buffer bytes/order mismatch") }
      return
     }
     journal,err:=openOperationJournal(b.TempDir())
     if err!=nil {b.Fatal(err)}
     defer journal.close()
     b.ResetTimer()
     for i:=0;i<b.N;i++ {
      if arm=="frozen" {err=journal.writeBatchAt(batch,operationJournalWitnessBytes)} else {err=journal.adversarialExactWriteBatchAt(batch,operationJournalWitnessBytes)}
      if err!=nil {b.Fatal(err)}
     }
     b.StopTimer()
     actual,err:=os.ReadFile(journal.path)
     if err!=nil {b.Fatal(err)}
     want:=append(journalWitnessLine(batch[n-1].revision,batch[n-1].hash),expected...)
     if !bytes.Equal(actual,want) {b.Fatal("actual file bytes/order/witness mismatch")}
     b.Logf("verified: %d records, %d payload bytes + %d witness bytes",n,len(expected),operationJournalWitnessBytes)
    })
   }
  }
 }
}
