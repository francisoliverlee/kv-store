package kvstore

import (
	"github.com/dgraph-io/badger/v4"
	"testing"
)

func Test_badgerStore_BuildKey(t *testing.T) {
	type fields struct {
		db   *badger.DB
		path string
		opts badger.Options
	}
	type args struct {
		prefix string
		keys   []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "test1",
			args: struct {
				prefix string
				keys   []string
			}{prefix: "cluster1", keys: []string{"1", "2"}},
			want: "cluster1@1@2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := badgerStore{
				db:   tt.fields.db,
				path: tt.fields.path,
				opts: tt.fields.opts,
			}
			if got, _ := b.BuildKey(tt.args.prefix, tt.args.keys...); got != tt.want {
				t.Errorf("BuildKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
