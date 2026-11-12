package worker

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// Lean CRI listings: the same calls as the CRI client's, read straight
// from the wire like the containerd listings in leanlist.go. A pod sandbox
// carries all of its pod's labels and annotations (a whole last-applied
// configuration, often), a container its own; the worker uses none of
// them, only ids, image references and states.

// leanContainers is RuntimeService.ListContainers, each container with
// only Id, PodSandboxId, Image.Image, Image.UserSpecifiedImage, ImageRef
// and State set.
func leanContainers(ctx context.Context, cc grpc.ClientConnInterface, req *runtimeapi.ListContainersRequest) ([]*runtimeapi.Container, error) {
	opt, err := leanCallOption()
	if err != nil {
		return nil, err
	}
	var out []*runtimeapi.Container
	err = cc.Invoke(ctx, runtimeapi.RuntimeService_ListContainers_FullMethodName, req, &leanReply{parse: func(b []byte) error {
		// ListContainersResponse: repeated Container containers = 1.
		return eachMessage(b, 1, func(m []byte) error {
			c, err := parseContainer(m)
			if err != nil {
				return err
			}
			out = append(out, c)
			return nil
		})
	}}, opt)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// parseContainer reads a Container: string id = 1, string pod_sandbox_id
// = 2, ImageSpec image = 4 (string image = 1, string user_specified_image
// = 18), string image_ref = 5, ContainerState state = 6.
func parseContainer(b []byte) (*runtimeapi.Container, error) {
	c := &runtimeapi.Container{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		b = b[n:]
		switch {
		case typ == protowire.BytesType && (num == 1 || num == 2 || num == 4 || num == 5):
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			b = b[n:]
			if num == 4 {
				if err := mergeImageSpec(c, v); err != nil {
					return nil, err
				}
				continue
			}
			s, err := utf8String(v)
			if err != nil {
				return nil, err
			}
			switch num {
			case 1:
				c.Id = s
			case 2:
				c.PodSandboxId = s
			case 5:
				c.ImageRef = s
			}
		case typ == protowire.VarintType && num == 6:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			b = b[n:]
			c.State = runtimeapi.ContainerState(int32(v))
		default:
			if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
				return nil, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return c, nil
}

// mergeImageSpec reads an ImageSpec into c.Image, merging as a full
// decode does when the field comes more than once.
func mergeImageSpec(c *runtimeapi.Container, b []byte) error {
	if c.Image == nil {
		c.Image = &runtimeapi.ImageSpec{}
	}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if typ == protowire.BytesType && (num == 1 || num == 18) {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return protowire.ParseError(n)
			}
			b = b[n:]
			s, err := utf8String(v)
			if err != nil {
				return err
			}
			if num == 1 {
				c.Image.Image = s
			} else {
				c.Image.UserSpecifiedImage = s
			}
			continue
		}
		if n = protowire.ConsumeFieldValue(num, typ, b); n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
	}
	return nil
}

// leanSandboxIDs is RuntimeService.ListPodSandbox, only the ids.
func leanSandboxIDs(ctx context.Context, cc grpc.ClientConnInterface, req *runtimeapi.ListPodSandboxRequest) ([]string, error) {
	opt, err := leanCallOption()
	if err != nil {
		return nil, err
	}
	var ids []string
	err = cc.Invoke(ctx, runtimeapi.RuntimeService_ListPodSandbox_FullMethodName, req, &leanReply{parse: func(b []byte) error {
		// ListPodSandboxResponse: repeated PodSandbox items = 1; PodSandbox: string id = 1.
		return eachMessage(b, 1, func(m []byte) error {
			id, err := stringField(m, 1)
			if err != nil {
				return err
			}
			ids = append(ids, id)
			return nil
		})
	}}, opt)
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// leanImageEntries is ImageService.ListImages, each image's id, tags and
// digests (Image: string id = 1, repeated string repo_tags = 2, repeated
// string repo_digests = 3).
func leanImageEntries(ctx context.Context, cc grpc.ClientConnInterface) ([]imageEntry, error) {
	opt, err := leanCallOption()
	if err != nil {
		return nil, err
	}
	var out []imageEntry
	err = cc.Invoke(ctx, runtimeapi.ImageService_ListImages_FullMethodName, &runtimeapi.ListImagesRequest{}, &leanReply{parse: func(b []byte) error {
		// ListImagesResponse: repeated Image images = 1.
		return eachMessage(b, 1, func(m []byte) error {
			var e imageEntry
			for len(m) > 0 {
				num, typ, n := protowire.ConsumeTag(m)
				if n < 0 {
					return protowire.ParseError(n)
				}
				m = m[n:]
				if typ == protowire.BytesType && num >= 1 && num <= 3 {
					v, n := protowire.ConsumeBytes(m)
					if n < 0 {
						return protowire.ParseError(n)
					}
					m = m[n:]
					s, err := utf8String(v)
					if err != nil {
						return err
					}
					switch num {
					case 1:
						e.ID = s
					case 2:
						e.RepoTags = append(e.RepoTags, s)
					case 3:
						e.RepoDigests = append(e.RepoDigests, s)
					}
					continue
				}
				if n = protowire.ConsumeFieldValue(num, typ, m); n < 0 {
					return protowire.ParseError(n)
				}
				m = m[n:]
			}
			out = append(out, e)
			return nil
		})
	}}, opt)
	if err != nil {
		return nil, err
	}
	return out, nil
}
