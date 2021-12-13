package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-go/tfprotov5"
	"github.com/hashicorp/terraform-plugin-go/tftypes"
	"github.com/hashicorp/terraform-provider-kubernetes/manifest/morph"
	"github.com/hashicorp/terraform-provider-kubernetes/manifest/payload"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ReadDataSource function
func (s *RawProviderServer) ReadDataSource(ctx context.Context, req *tfprotov5.ReadDataSourceRequest) (*tfprotov5.ReadDataSourceResponse, error) {
	s.logger.Trace("[ReadDataSource][Request]\n%s\n", dump(*req))

	resp := &tfprotov5.ReadDataSourceResponse{}

	execDiag := s.canExecute()
	if len(execDiag) > 0 {
		resp.Diagnostics = append(resp.Diagnostics, execDiag...)
		return resp, nil
	}

	rt, err := GetDataSourceType(req.TypeName)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to determine data source type",
			Detail:   err.Error(),
		})
		return resp, nil
	}

	config, err := req.Config.Unmarshal(rt)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to unmarshal data source configuration",
			Detail:   err.Error(),
		})
		return resp, nil
	}

	var dsConfig map[string]tftypes.Value
	err = config.As(&dsConfig)
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to extract attributes from data source configuration",
			Detail:   err.Error(),
		})
		return resp, nil
	}

	rm, err := s.getRestMapper()
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "Failed to get RESTMapper client",
			Detail:   err.Error(),
		})
		return resp, nil
	}
	client, err := s.getDynamicClient()
	if err != nil {
		resp.Diagnostics = append(resp.Diagnostics, &tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  "failed to get Dynamic client",
			Detail:   err.Error(),
		})
		return resp, nil
	}

	var apiVersion, kind string
	dsConfig["apiVersion"].As(&apiVersion)
	dsConfig["kind"].As(&kind)

	gvr, err := getGVR(apiVersion, kind, rm)
	if err != nil {
		return resp, err
	}
	gvk := gvr.GroupVersion().WithKind(kind)
	ns, err := IsResourceNamespaced(gvk, rm)
	if err != nil {
		return resp, err
	}
	rcl := client.Resource(gvr)

	var metadata map[string]tftypes.Value
	dsConfig["metadata"].As(&metadata)

	var name string
	metadata["name"].As(&name)

	var res *unstructured.Unstructured
	if ns {
		var namespace string
		metadata["namespace"].As(&namespace)
		res, err = rcl.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	} else {
		res, err = rcl.Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			return resp, nil
		}
		d := tfprotov5.Diagnostic{
			Severity: tfprotov5.DiagnosticSeverityError,
			Summary:  fmt.Sprintf("Failed to get data source"),
			Detail:   err.Error(),
		}
		resp.Diagnostics = append(resp.Diagnostics, &d)
		return resp, nil
	}

	objectType, err := s.TFTypeFromOpenAPI(ctx, gvk, false)
	if err != nil {
		return resp, fmt.Errorf("failed to determine data source type: %s", err)
	}

	fo := RemoveServerSideFields(res.Object)
	nobj, err := payload.ToTFValue(fo, objectType, tftypes.NewAttributePath())
	if err != nil {
		return resp, err
	}

	nobj, err = morph.DeepUnknown(objectType, nobj, tftypes.NewAttributePath())
	if err != nil {
		return resp, err
	}
	rawState := make(map[string]tftypes.Value)
	err = config.As(&rawState)
	if err != nil {
		return resp, err
	}
	rawState["object"] = morph.UnknownToNull(nobj)

	v := tftypes.NewValue(rt, rawState)
	state, err := tfprotov5.NewDynamicValue(v.Type(), v)
	if err != nil {
		return resp, err
	}
	resp.State = &state
	return resp, nil
}

func getGVR(apiVersion, kind string, m meta.RESTMapper) (schema.GroupVersionResource, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	mapping, err := m.RESTMapping(gv.WithKind(kind).GroupKind(), gv.Version)
	if err != nil {
		return schema.GroupVersionResource{}, err
	}
	return mapping.Resource, err
}
