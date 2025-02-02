package codegen

import (
	"path/filepath"
	"reflect"
	"strings"

	"github.com/cortexproject/cortex/pkg/compactor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/builder"
	"github.com/jhump/protoreflect/desc/protoprint"
	"github.com/rancher/opni/internal/codegen/cli"
	"github.com/rancher/opni/internal/codegen/descriptors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func GenCortexConfig() error {
	return nil
}

// from cortex doc-generator
func parseDocTag(f reflect.StructField) map[string]string {
	cfg := map[string]string{}
	tag := f.Tag.Get("doc")

	if tag == "" {
		return cfg
	}

	for _, entry := range strings.Split(tag, "|") {
		parts := strings.SplitN(entry, "=", 2)

		switch len(parts) {
		case 1:
			cfg[parts[0]] = ""
		case 2:
			cfg[parts[0]] = parts[1]
		}
	}

	return cfg
}

func editFlagOptions(rf reflect.StructField) func(*cli.FlagOptions) {
	docInfo := parseDocTag(rf)
	if _, ok := docInfo["nocli"]; ok {
		return func(fo *cli.FlagOptions) {
			fo.Skip = true
		}
	}
	return nil
}

func editFieldComment(rf reflect.StructField, in *string) {
	docInfo := parseDocTag(rf)
	if doc, ok := docInfo["description"]; ok {
		*in = doc
	}
}

var cortexTypesToSkip = map[reflect.Type]bool{
	reflect.TypeOf(compactor.RingConfig{}): true,
}

func generate[T any](destFilename string, skipFunc ...func(rf reflect.StructField) bool) error {
	messages := descriptors.BuildMessage[T](descriptors.BuilderOptions{
		FieldNameFromTags: []string{"json", "yaml"},
		EditFlagOptions:   editFlagOptions,
		EditFieldComment:  editFieldComment,
		SkipFieldFunc: func(rf reflect.StructField) bool {
			if cortexTypesToSkip[rf.Type] {
				return true
			}
			if strings.HasPrefix(rf.Name, "Sharding") {
				return true
			}
			if strings.HasSuffix(rf.Name, "Dir") {
				return true
			}
			if _, ok := parseDocTag(rf)["hidden"]; ok {
				return true
			}
			if len(skipFunc) > 0 {
				return skipFunc[0](rf)
			}
			return false
		},
	})
	f := builder.NewFile(filepath.Base(destFilename)).
		SetProto3(true).
		SetPackageName(filepath.Base(filepath.Dir(destFilename))).
		SetOptions(&descriptorpb.FileOptions{
			GoPackage: lo.ToPtr(filepath.Dir(destFilename)),
		}).
		SetSyntaxComments(builder.Comments{
			LeadingComment: "Code generated by internal/codegen. DO NOT EDIT.",
		})
	proto.SetExtension(f.Options, cli.E_Generator, &cli.GeneratorOptions{
		Generate:         true,
		GenerateDeepcopy: true,
	})
	for _, m := range messages {
		f.AddMessage(m)
	}
	fd, err := f.Build()
	if err != nil {
		return err
	}
	p := protoprint.Printer{
		Compact:      true,
		SortElements: true,
	}
	return p.PrintProtosToFileSystem([]*desc.FileDescriptor{fd}, strings.TrimPrefix(filepath.Dir(destFilename), "github.com/rancher/opni/"))
}
