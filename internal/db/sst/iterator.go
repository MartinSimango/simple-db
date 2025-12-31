package sst

type Iterator struct {
	r          *Reader
	block      *DataBlock
	blockIndex int
	entryIndex int
	prevKey    string
	err        error
}

func NewIterator(r *Reader) *Iterator {
	return &Iterator{
		r:          r,
		block:      nil,
		blockIndex: -1,
		entryIndex: -1,
		err:        nil,
	}
}

func (i *Iterator) Entry() *BlockEntry {
	if i.block == nil || i.err != nil {
		return nil
	}
	return i.block.DataBlockEntries.Entries[i.entryIndex]
}

func (i *Iterator) Key() []byte {
	blockEntry := i.Entry()
	if blockEntry == nil {
		return nil
	}
	key := i.prevKey[:blockEntry.SharedKeyLen] + string(blockEntry.UnsharedKey)

	return []byte(key)

}

func (i *Iterator) Next() bool {
	if i.err != nil {

		return false
	}

	var err error
	if i.block == nil || i.entryIndex+1 >= len(i.block.DataBlockEntries.Entries) {
		i.blockIndex++
		i.entryIndex = 0
		i.prevKey = ""
		if i.block, err = i.r.readDataBlock(i.blockIndex); err != nil {
			i.err = err

			return false
		}
		return true
	}

	entry := i.block.DataBlockEntries.Entries[i.entryIndex]
	i.prevKey = i.prevKey[:entry.SharedKeyLen] + string(entry.UnsharedKey)
	i.entryIndex++

	return true
}

func (i *Iterator) Error() error {
	return i.err
}
