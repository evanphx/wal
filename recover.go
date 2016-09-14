package wal

func BeginRecovery(path string, tag []byte) (*WALReader, error) {
	r, err := NewReader(path)
	if err != nil {
		return nil, err
	}

	pos, err := r.SeekTag(tag)
	if err != nil {
		r.Close()
		return nil, err
	}

	if pos.None() {
		err = r.Reset()
		if err != nil {
			r.Close()
			return nil, err
		}
	}

	return r, nil
}
