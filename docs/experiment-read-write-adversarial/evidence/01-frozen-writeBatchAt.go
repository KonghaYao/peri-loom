func (journal *operationJournal) writeBatchAt(batch []*operationCommit, offset int64) error {
	if journal.testWriteGate != nil {
		journal.testWriteGate()
	}
	if err := journal.verifyPathIdentity(); err != nil {
		return ErrStateUncertain
	}
	run := make([]byte, 0, operationJournalBatchBytes)
	for _, commit := range batch {
		run = append(run, commit.line...)
	}
	if _, err := journal.file.WriteAt(run, offset); err != nil {
		return ErrStateUncertain
	}
	if err := journal.file.Sync(); err != nil {
		return ErrStateUncertain
	}
	return journal.writeWitnessFor(batch[len(batch)-1])
}
