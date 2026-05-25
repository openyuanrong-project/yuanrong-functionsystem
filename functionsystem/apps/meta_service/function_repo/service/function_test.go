/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"meta_service/common/constants"
	common "meta_service/common/constants"
	"meta_service/common/snerror"
	"meta_service/function_repo/errmsg"
	"meta_service/function_repo/model"
	"meta_service/function_repo/pkgstore"
	"meta_service/function_repo/server"
	"meta_service/function_repo/storage"
	"meta_service/function_repo/storage/publish"
	"meta_service/function_repo/test/fakecontext"
	"meta_service/function_repo/utils"
)

func TestCreateFunctionInfo(t *testing.T) {
	type args struct {
		ctx server.Context
		req model.FunctionCreateRequest
	}
	tests := []struct {
		name    string
		args    args
		want    model.FunctionVersion
		wantErr bool
	}{
		{"test create success", args{fakecontext.NewMockContext(), model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				CPU:           500,
				Memory:        500,
				Timeout:       500,
				MinInstance:   "10",
				MaxInstance:   "10",
				ConcurrentNum: "10",
				Handler:       "main",
				Kind:          "faas",
				Environment:   nil,
				SchedulePolicies: []model.ResourceAffinitySelector{
					{Group: "rg1"},
				},
				PoolID:     "pool1",
				PriorityAZ: "az1",
			},
			Name:    "successFunc",
			Runtime: "java1.8",
			Tags:    nil,
		}}, model.FunctionVersion{}, false},
		{"test layer error", args{fakecontext.NewMockContext(), model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				CPU:           500,
				Memory:        500,
				Timeout:       500,
				MinInstance:   "10",
				MaxInstance:   "10",
				ConcurrentNum: "10",
				Layers:        []string{"aa", "bb"},
				Handler:       "main",
				Environment:   nil,
			},
			Name:    "layererror",
			Runtime: "java1.8",
			Tags:    nil,
		}}, model.FunctionVersion{
			Status: constants.FunctionStatusAvailable,
		}, true},
		{"test create name exist", args{fakecontext.NewMockContext(), model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				CPU:           500,
				Memory:        500,
				Timeout:       500,
				MinInstance:   "10",
				MaxInstance:   "10",
				ConcurrentNum: "10",
				Handler:       "main",
				Environment:   nil,
			},
			Name:    "successFunc",
			Runtime: "java1.8",
			Tags:    nil,
		}}, model.FunctionVersion{}, false},
	}
	// test success
	tt := tests[0]
	t.Run(tt.name, func(t *testing.T) {
		got, _ := CreateFunctionInfo(tt.args.ctx, tt.args.req, false)
		if got.RevisionID == "" {
			t.Logf("RevisionID %s", got.RevisionID)
			t.Errorf("CreateFunctionInfo() got = %v", got)
		}
		if got.FunctionName != tt.args.req.Name {
			t.Errorf("CreateFunctionInfo got function %s", got.FunctionName)
		}
		if got.VersionNumber == "" {
			t.Errorf("CreateFunctionInfo got version empty %s", got.VersionNumber)
		}
		assert.Equal(t, got.Runtime, "java8")
		assert.Equal(t, "az1", got.PriorityAZ)
	})
	// test layer
	tt = tests[1]
	t.Run(tt.name, func(t *testing.T) {
		_, err := CreateFunctionInfo(tt.args.ctx, tt.args.req, false)
		t.Logf("err: %s", err)
		assert.NotEqual(t, err, nil)
	})
	// test name exist
	tt = tests[2]
	t.Run(tt.name, func(t *testing.T) {
		_, err := CreateFunctionInfo(tt.args.ctx, tt.args.req, false)
		_, err = CreateFunctionInfo(tt.args.ctx, tt.args.req, false)
		t.Logf("err: %s", err)
		assert.NotEqual(t, err, nil)
	})
}

func TestUpdateFunctionInfo(t *testing.T) {
	type args struct {
		ctx server.Context
		f   model.FunctionQueryInfo
		fv  model.FunctionUpdateRequest
	}
	tests := []struct {
		name    string
		args    args
		want    model.FunctionVersion
		wantErr bool
	}{
		{
			"test update success",
			args{
				fakecontext.NewMockContext(),
				model.FunctionQueryInfo{
					FunctionName:    "0-test-successFunc",
					FunctionVersion: "$latest",
					AliasName:       "",
				},
				model.FunctionUpdateRequest{
					FunctionBasicInfo: model.FunctionBasicInfo{
						CPU:           500,
						Memory:        100,
						Timeout:       100,
						Layers:        nil,
						MinInstance:   "10",
						MaxInstance:   "10",
						ConcurrentNum: "10",
						Handler:       "main",
						Environment:   nil,
						Description:   "test update success",
						PriorityAZ:    "az2",
					},
					RevisionID: "",
				}},
			model.FunctionVersion{},
			false,
		},
	}
	// test success
	tt := tests[0]
	t.Run(tt.name, func(t *testing.T) {
		mockCreateFunction(t)
		// get RevisionID not same
		_, err := UpdateFunctionInfo(tt.args.ctx, tt.args.f, tt.args.fv, false)
		assert.NotEqual(t, err, nil)
		// test success
		getFunc := mockGetFunc(t, tt.args.ctx, tt.args.f.FunctionName)
		tt.args.fv.RevisionID = getFunc.RevisionID
		got, err := UpdateFunctionInfo(tt.args.ctx, tt.args.f, tt.args.fv, false)
		if (err != nil) != tt.wantErr {
			t.Errorf("UpdateFunctionInfo() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		assert.NotEqual(t, got.RevisionID, nil)
		// test update value
		getFunc = mockGetFunc(t, tt.args.ctx, tt.args.f.FunctionName)
		assert.NotEqual(t, createReq.Timeout, getFunc.Timeout)
		assert.Equal(t, int64(100), getFunc.Timeout)
		assert.Equal(t, "az2", getFunc.PriorityAZ)
		assert.Nil(t, err)
		err = DeleteResponse(tt.args.ctx, "0-test-successFunc", utils.GetDefaultVersion(), "")
		txn := storage.GetTxnByKind(tt.args.ctx, "")
		defer txn.Cancel()
		deleteAllFunctions(txn, "0-test-successFunc")
		assert.Nil(t, err)
	})
}

func mockCreateFunction(t *testing.T) {
	_, err := CreateFunctionInfo(fakecontext.NewMockContext(), createReq, false)
	if err != nil {
		t.Errorf("CreateFunctionInfo err %v", err)
	}
}

func mockGetFunc(t *testing.T, ctx server.Context, name string) model.FunctionVersion {
	rep, err := GetFunction(ctx, model.FunctionGetRequest{
		FunctionName:    name,
		FunctionVersion: "",
		AliasName:       "",
	})
	if err != nil {
		t.Errorf("MockGetFunc err %s", err)
	}
	return rep.FunctionVersion
}

var createReq = model.FunctionCreateRequest{
	FunctionBasicInfo: model.FunctionBasicInfo{
		CPU:           500,
		Memory:        500,
		Timeout:       500,
		MinInstance:   "10",
		MaxInstance:   "10",
		ConcurrentNum: "10",
		Handler:       "main",
		Environment:   nil,
	},
	Name:    "0-test-successFunc",
	Runtime: "java1.8",
	Tags:    nil,
}

func TestGetFunctionVersionList(t *testing.T) {
	type args struct {
		ctx server.Context
	}
	tests := []struct {
		name    string
		args    args
		want    model.FunctionVersion
		wantErr bool
	}{
		{"test get version success", args{fakecontext.NewMockContext()},
			model.FunctionVersion{}, false},
	}
	tt := tests[0]
	t.Run(tt.name, func(t *testing.T) {
		_, err := GetFunctionVersionList(tt.args.ctx, "name", 3, 4)
		assert.Equal(t, err, nil)
	})
}

func TestGetFunctionList(t *testing.T) {
	type args struct {
		ctx server.Context
	}
	tests := []struct {
		name    string
		args    args
		want    model.FunctionVersion
		wantErr bool
	}{
		{"test get func success", args{fakecontext.NewMockContext()},
			model.FunctionVersion{}, false},
	}
	tt := tests[0]
	t.Run(tt.name, func(t *testing.T) {
		_, err := GetFunctionList(tt.args.ctx, "name", "", "", 4, 1024)
		assert.Equal(t, err, nil)
	})
}

func getZipBody(t *testing.T) *http.Request {
	zip, err := base64.StdEncoding.DecodeString("UEsDBBQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEAAAAc3JjL1BLAQI/ABQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEACQAAAAAAAAAEAAAAAAAAABzcmMvCgAgAAAAAAABABgA/3JrC45F2AHLUrYMjkXYAf9yawuORdgBUEsFBgAAAAABAAEAVgAAACIAAAAAAA==")
	assert.Nil(t, err)
	return &http.Request{Body: ioutil.NopCloser(bytes.NewReader(zip))}
}

func TestGetUploadFunctionCodeResponse(t *testing.T) {
	ctx := fakecontext.NewMockContext()
	getFunc := mockGetFunc(t, ctx, "0-test-successFunc")
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(getUploadFile, func(_ server.Context) (io.Reader, error) {
		zip, _ := base64.StdEncoding.DecodeString("UEsDBBQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEAAAAc3JjL1BLAQI/ABQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEACQAAAAAAAAAEAAAAAAAAABzcmMvCgAgAAAAAAABABgA/3JrC45F2AHLUrYMjkXYAf9yawuORdgBUEsFBgAAAAABAAEAVgAAACIAAAAAAA==")
		return ioutil.NopCloser(bytes.NewReader(zip)), nil
	})
	defer patches.Reset()
	RevisionID := getFunc.RevisionID
	_, err := GetUploadFunctionCodeResponse(ctx, model.FunctionQueryInfo{
		FunctionName:    "0-test-successFunc",
		FunctionVersion: "$latest",
		AliasName:       ""},
		model.FunctionCodeUploadRequest{RevisionID: RevisionID,
			FileSize: 142},
	)
	assert.Nil(t, err)
	err = DeleteResponse(ctx, "0-test-successFunc", utils.GetDefaultVersion(), "")
	txn := storage.GetTxnByKind(ctx, "")
	defer txn.Cancel()
	deleteAllFunctions(txn, "0-test-successFunc")
	assert.Nil(t, err)
}

func TestDeleteResponse(t *testing.T) {
	type args struct {
		ctx server.Context
		req model.FunctionCreateRequest
	}
	tests := []struct {
		name    string
		args    args
		want    model.FunctionVersion
		wantErr bool
	}{
		{"test delete success", args{fakecontext.NewMockContext(), model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				CPU:           500,
				Memory:        500,
				Timeout:       500,
				MinInstance:   "10",
				MaxInstance:   "10",
				ConcurrentNum: "10",
				Handler:       "main",
				Environment:   nil,
			},
			Name:    "deleteFunc",
			Runtime: "java1.8",
			Tags:    nil,
		}}, model.FunctionVersion{}, false},
	}
	// del
	tt := tests[0]
	t.Run(tt.name, func(t *testing.T) {
		got, _ := CreateFunctionInfo(tt.args.ctx, tt.args.req, false)
		if got.RevisionID == "" {
			t.Logf("RevisionID %s", got.RevisionID)
			t.Errorf("CreateFunctionInfo() got = %v", got)
		}
		err := DeleteResponse(tt.args.ctx, "deleteFunc", utils.GetDefaultVersion(), "")
		txn := storage.GetTxnByKind(tt.args.ctx, "")
		defer txn.Cancel()
		deleteAllFunctions(txn, "deleteFunc")
		assert.Equal(t, err, nil)
	})
}

func TestGetUploadFunctionCodeResponse2(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(getUploadFile, func(_ server.Context) (io.Reader, error) {
		zip, _ := base64.StdEncoding.DecodeString("UEsDBBQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEAAAAc3JjL1BLAQI/ABQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEACQAAAAAAAAAEAAAAAAAAABzcmMvCgAgAAAAAAABABgA/3JrC45F2AHLUrYMjkXYAf9yawuORdgBUEsFBgAAAAABAAEAVgAAACIAAAAAAA==")
		return ioutil.NopCloser(bytes.NewReader(zip)), nil
	})
	defer patches.Reset()
	ctx := fakecontext.NewMockContext()
	getFunc := mockGetFunc(t, ctx, "0-test-successFunc")
	zip, err := base64.StdEncoding.DecodeString("")
	assert.Nil(t, err)
	ctx.Gin().Request = &http.Request{Body: ioutil.NopCloser(bytes.NewReader(zip))}
	RevisionID := getFunc.RevisionID
	_, err = GetUploadFunctionCodeResponse(ctx, model.FunctionQueryInfo{
		FunctionName:    "0-test-successFunc",
		FunctionVersion: "$latest",
		AliasName:       ""},
		model.FunctionCodeUploadRequest{RevisionID: RevisionID,
			FileSize: 0},
	)
	assert.NotNil(t, err)
	assert.Equal(t, errmsg.ZipFileError, err.(snerror.SNError).Code())
	err = DeleteResponse(ctx, "0-test-successFunc", utils.GetDefaultVersion(), "")
	txn := storage.GetTxnByKind(ctx, "")
	defer txn.Cancel()
	deleteAllFunctions(txn, "0-test-successFunc")
	assert.Nil(t, err)
}

func TestGetUploadFunctionCodeResponse3(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(getUploadFile, func(_ server.Context) (io.Reader, error) {
		zip, _ := base64.StdEncoding.DecodeString("UEsDBBQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEAAAAc3JjL1BLAQI/ABQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEACQAAAAAAAAAEAAAAAAAAABzcmMvCgAgAAAAAAABABgA/3JrC45F2AHLUrYMjkXYAf9yawuORdgBUEsFBgAAAAABAAEAVgAAACIAAAAAAA==")
		return ioutil.NopCloser(bytes.NewReader(zip)), nil
	})
	defer patches.Reset()
	ctx := fakecontext.NewMockContext()

	ctx.Gin().Request = getZipBody(t)
	getFunc := mockGetFunc(t, ctx, "0-test-successFunc")
	RevisionID := getFunc.RevisionID
	_, err := GetUploadFunctionCodeResponse(ctx, model.FunctionQueryInfo{
		FunctionName:    "0-test-successFunc",
		FunctionVersion: "$latest",
		AliasName:       ""},
		model.FunctionCodeUploadRequest{RevisionID: RevisionID,
			FileSize: 142},
	)
	assert.Nil(t, err)
}

func TestGetUploadFunctionCodeResponse4(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(getUploadFile, func(_ server.Context) (io.Reader, error) {
		zip, _ := base64.StdEncoding.DecodeString("UEsDBBQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEAAAAc3JjL1BLAQI/ABQAAAAAAEpwgVQAAAAAAAAAAAAAAAAEACQAAAAAAAAAEAAAAAAAAABzcmMvCgAgAAAAAAABABgA/3JrC45F2AHLUrYMjkXYAf9yawuORdgBUEsFBgAAAAABAAEAVgAAACIAAAAAAA==")
		return ioutil.NopCloser(bytes.NewReader(zip)), nil
	})
	defer patches.Reset()
	ctx := fakecontext.NewMockContext()
	ctx.Gin().Request = getZipBody(t)
	_, err := GetUploadFunctionCodeResponse(ctx, model.FunctionQueryInfo{
		FunctionName:    "0-test-successFunc",
		FunctionVersion: "$latest",
		AliasName:       ""},
		model.FunctionCodeUploadRequest{RevisionID: "RevisionID",
			FileSize: 142},
	)
	assert.NotNil(t, err)
	assert.Equal(t, errmsg.RevisionIDError, err.(snerror.SNError).Code())
	err = DeleteResponse(ctx, "0-test-successFunc", utils.GetDefaultVersion(), "")
	txn := storage.GetTxnByKind(ctx, "")
	defer txn.Cancel()
	deleteAllFunctions(txn, "0-test-successFunc")
	assert.Nil(t, err)
}

type mockUploaderBuilder struct{}

// NewUploader implements UploaderBuilder
func (mockUploaderBuilder) NewUploader(c server.Context, pkg pkgstore.Package) (pkgstore.Uploader, error) {
	return mockUploader{}, nil
}

type mockUploader struct {
	mockRollbackErr error
}

// BucketID implements Uploader
func (mockUploader) BucketID() string {
	return ""
}

// ObjectID implements Uploader
func (mockUploader) ObjectID() string {
	return ""
}

// Upload implements Uploader
func (mockUploader) Upload() error {
	return nil
}

// Rollback implements Uploader
func (mu mockUploader) Rollback() error {
	return mu.mockRollbackErr
}

func TestFailUploadFunctionCodeHelper(t *testing.T) {
	Convey("test failUploadFunctionCodeHelper", t, func() {
		ctx := fakecontext.NewMockContext()
		patch := gomonkey.ApplyFunc(storage.RemoveUncontrolled, func(ctx server.Context, bucketID, objectID string) error {
			return nil
		})
		defer patch.Reset()
		noopUploader := mockUploader{}
		noopUploader.mockRollbackErr = nil
		_, err := failUploadFunctionCodeHelper(ctx, noopUploader, false, errors.New("mock err"))
		So(err, ShouldNotBeNil)
		noopUploader.mockRollbackErr = errors.New("mock err")
		_, err = failUploadFunctionCodeHelper(ctx, noopUploader, false, errors.New("mock err"))
		So(err, ShouldNotBeNil)
	})
}

func TestGetUploadFile(t *testing.T) {
	Convey("Test getUploadFile with ParseMultipartForm err", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&http.Request{}), "ParseMultipartForm",
			func(r *http.Request, maxMemory int64) error {
				return errors.New("mock err")
			})
		defer patch.Reset()
		ctx := fakecontext.NewMockContext()
		ctx.Gin().Request = &http.Request{}
		ctx.Gin().Request.Header = make(map[string][]string)
		ctx.Gin().Request.Header.Set(common.HeaderDataContentType, "mock-data")
		_, err := getUploadFile(ctx)
		So(err, ShouldNotBeNil)
	})

	Convey("Test getUploadFile with multipartForm is empty err", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&http.Request{}), "ParseMultipartForm",
			func(r *http.Request, maxMemory int64) error {
				return nil
			})
		defer patch.Reset()
		ctx := fakecontext.NewMockContext()
		ctx.Gin().Request = &http.Request{}
		ctx.Gin().Request.Header = make(map[string][]string)
		ctx.Gin().Request.Header.Set(common.HeaderDataContentType, "mock-data")
		ctx.Gin().Request.MultipartForm = &multipart.Form{File: map[string][]*multipart.FileHeader{
			"file": []*multipart.FileHeader{},
		}}
		_, err := getUploadFile(ctx)
		So(err, ShouldNotBeNil)
	})

	Convey("Test getUploadFile with Open err", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&http.Request{}), "ParseMultipartForm",
			func(r *http.Request, maxMemory int64) error {
				return nil
			}).ApplyMethod(reflect.TypeOf(&multipart.FileHeader{}), "Open",
			func(*multipart.FileHeader) (multipart.File, error) {
				return &os.File{}, errors.New("mock err")
			})
		defer patch.Reset()
		ctx := fakecontext.NewMockContext()
		ctx.Gin().Request = &http.Request{}
		ctx.Gin().Request.Header = make(map[string][]string)
		ctx.Gin().Request.Header.Set(common.HeaderDataContentType, "mock-data")
		ctx.Gin().Request.MultipartForm = &multipart.Form{File: map[string][]*multipart.FileHeader{
			"file": []*multipart.FileHeader{{}},
		}}
		_, err := getUploadFile(ctx)
		So(err, ShouldNotBeNil)
	})
	Convey("Test getUploadFile", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&http.Request{}), "ParseMultipartForm",
			func(r *http.Request, maxMemory int64) error {
				return nil
			}).ApplyMethod(reflect.TypeOf(&multipart.FileHeader{}), "Open",
			func(*multipart.FileHeader) (multipart.File, error) {
				return &os.File{}, errors.New("mock err")
			}).ApplyMethod(reflect.TypeOf(&multipart.FileHeader{}), "Open",
			func(*multipart.FileHeader) (multipart.File, error) {
				return &os.File{}, nil
			})
		defer patch.Reset()
		ctx := fakecontext.NewMockContext()
		ctx.Gin().Request = &http.Request{}
		ctx.Gin().Request.Header = make(map[string][]string)
		ctx.Gin().Request.Header.Set(common.HeaderDataContentType, "mock-data")
		ctx.Gin().Request.MultipartForm = &multipart.Form{File: map[string][]*multipart.FileHeader{
			"file": []*multipart.FileHeader{{}},
		}}
		_, err := getUploadFile(ctx)
		So(err, ShouldBeNil)
	})
}

func TestBuildEnvironment(t *testing.T) {
	Convey("Test buildEnvironment", t, func() {
		v := storage.FunctionVersionValue{
			Function: storage.Function{},
			FunctionVersion: storage.FunctionVersion{
				Environment: "mock env",
			},
			FunctionLayer: nil,
		}
		gomonkey.ApplyFunc(utils.DecryptETCDValue, func(text string) (string, error) {
			return text, errors.New("mock err")
		})
		buildEnvironment(nil, v)
	})
}

func TestUpdateFunctionLayer(t *testing.T) {
	Convey("Test updateFunctionLayer 1", t, func() {
		request := model.FunctionUpdateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				Layers: []string{"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:1"},
			},
		}
		fv := &storage.FunctionVersionValue{FunctionLayer: make([]storage.FunctionLayer, 0)}
		err := updateFunctionLayer(request, fv)
		So(err, ShouldBeNil)
	})
	Convey("Test updateFunctionLayer 2", t, func() {
		request := model.FunctionUpdateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				Layers: []string{"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:a"},
			},
		}
		fv := &storage.FunctionVersionValue{FunctionLayer: make([]storage.FunctionLayer, 0)}
		err := updateFunctionLayer(request, fv)
		So(err, ShouldNotBeNil)
	})
}

func TestBuildFunctionVersion(t *testing.T) {
	Convey("Test buildFunctionVersion with err", t, func() {
		request := model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				Layers: []string{
					"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:1",
					"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:x",
				},
			},
		}
		_, err := buildFunctionVersion(request)
		So(err, ShouldNotBeNil)
	})
	Convey("Test buildFunctionVersion with EncodeEnv err", t, func() {
		patch := gomonkey.ApplyFunc(utils.EncodeEnv, func(m map[string]string) (string, error) {
			return "", errors.New("mock err")
		})
		defer patch.Reset()
		request := model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				Layers: []string{
					"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:1",
				},
				Environment: map[string]string{},
			},
		}
		_, err := buildFunctionVersion(request)
		So(err, ShouldNotBeNil)
	})
	Convey("Test buildFunctionVersion with Marshal err", t, func() {
		patch := gomonkey.ApplyFunc(utils.EncodeEnv, func(m map[string]string) (string, error) {
			return "", nil
		}).ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
			return nil, errors.New("mock err")
		})
		defer patch.Reset()
		request := model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				Layers: []string{
					"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:1",
				},
				Environment:     map[string]string{},
				CustomResources: map[string]float64{},
			},
		}
		_, err := buildFunctionVersion(request)
		So(err, ShouldNotBeNil)
	})
	Convey("Test buildFunctionVersion with functionVersionResult err", t, func() {
		patch := gomonkey.ApplyFunc(utils.EncodeEnv, func(m map[string]string) (string, error) {
			return "", nil
		})
		defer patch.Reset()
		request := model.FunctionCreateRequest{
			FunctionBasicInfo: model.FunctionBasicInfo{
				Layers: []string{
					"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:layer:aa:1",
				},
				Environment:     map[string]string{},
				CustomResources: map[string]float64{},
			},
		}
		_, err := buildFunctionVersion(request)
		So(err, ShouldNotBeNil)
	})
	Convey("Test buildFunctionVersion success with hook handler", t, func() {
		request := model.FunctionCreateRequest{
			Name:    "0@base@hello",
			Runtime: "java1.8",
			Kind:    "faas",
			FunctionBasicInfo: model.FunctionBasicInfo{
				CPU:           500,
				Memory:        500,
				Timeout:       30,
				ConcurrentNum: "100",
				Handler:       "test.handler",
				MinInstance:   "0",
				MaxInstance:   "100",
				StorageType:   "local",
				CodePath:      "/home",
			},
		}
		fv, err := buildFunctionVersion(request)
		So(err, ShouldBeNil)
		So(len(fv.FunctionVersion.HookHandler) > 0, ShouldBeTrue)
		So(fv.FunctionVersion.Runtime, ShouldEqual, "java8")
		request = model.FunctionCreateRequest{
			Name:    "0-base-hello",
			Runtime: "java8",
			Kind:    "yrlib",
			FunctionBasicInfo: model.FunctionBasicInfo{
				CPU:           500,
				Memory:        500,
				Timeout:       30,
				ConcurrentNum: "100",
				Handler:       "test.handler",
				MinInstance:   "0",
				MaxInstance:   "100",
				StorageType:   "local",
				CodePath:      "/home",
			},
		}
		fv, err = buildFunctionVersion(request)
		So(err, ShouldBeNil)
		So(fv.FunctionVersion.Runtime, ShouldEqual, "java1.8")
		So(len(fv.FunctionVersion.HookHandler) > 0, ShouldBeTrue)
	})
}

func TestGetUploadFunctionCodeHelper(t *testing.T) {
	Convey("Test getUploadFunctionCodeHelper", t, func() {
		txn := &storage.Txn{}
		fv := storage.FunctionVersionValue{}
		patches := gomonkey.ApplyFunc(storage.UpdateFunctionVersion,
			func(storage.Transaction, storage.FunctionVersionValue) error {
				return errors.New("mock err")
			})
		_, err := getUploadFunctionCodeHelper(txn, fv, "mock-bucket-id", "mock=obj-id")
		So(err, ShouldNotBeNil)
		patches.Reset()

		patches = gomonkey.ApplyFunc(storage.UpdateFunctionVersion,
			func(storage.Transaction, storage.FunctionVersionValue) error {
				return nil
			}).ApplyFunc(publish.SavePublishFuncVersion, func(txn storage.Transaction,
			funcVersionValue storage.FunctionVersionValue) error {
			return errors.New("mock err")
		})
		_, err = getUploadFunctionCodeHelper(txn, fv, "mock-bucket-id", "mock=obj-id")
		So(err, ShouldNotBeNil)
		patches.Reset()

		patches = gomonkey.ApplyFunc(storage.UpdateFunctionVersion,
			func(storage.Transaction, storage.FunctionVersionValue) error {
				return nil
			}).ApplyFunc(publish.SavePublishFuncVersion, func(txn storage.Transaction,
			funcVersionValue storage.FunctionVersionValue) error {
			return nil
		})
		defer patches.Reset()

		Convey("when IsObjectReferred err and Commit err", func() {
			patch2 := gomonkey.ApplyFunc(storage.IsObjectReferred, func(storage.Transaction, string, string) (bool, error) {
				return false, errors.New("mock err")
			}).ApplyMethod(reflect.TypeOf(txn), "Commit", func(_ *storage.Txn) error {
				return errors.New("mock commit err")
			})
			defer patch2.Reset()
			_, err = getUploadFunctionCodeHelper(txn, fv, "mock-bucket-id", "mock=obj-id")
			So(err, ShouldNotBeNil)
		})

		Convey("when pkgstore.Delete err and Commit err", func() {
			patch2 := gomonkey.ApplyFunc(storage.IsObjectReferred, func(storage.Transaction, string, string) (bool, error) {
				return false, nil
			}).ApplyFunc(pkgstore.Delete, func(bucketID, objectID string) error {
				return errors.New("mock err")
			}).ApplyMethod(reflect.TypeOf(txn), "Commit", func(_ *storage.Txn) error {
				return errors.New("mock commit err")
			})
			defer patch2.Reset()
			_, err = getUploadFunctionCodeHelper(txn, fv, "mock-bucket-id", "mock=obj-id")
			So(err, ShouldNotBeNil)
		})

		Convey("when TenantInfo err", func() {
			patch2 := gomonkey.ApplyFunc(storage.IsObjectReferred, func(storage.Transaction, string, string) (bool, error) {
				return false, nil
			}).ApplyFunc(pkgstore.Delete, func(bucketID, objectID string) error {
				return nil
			}).ApplyMethod(reflect.TypeOf(txn), "Commit", func(_ *storage.Txn) error {
				return nil
			}).ApplyMethod(reflect.TypeOf(txn), "GetCtx", func(tx *storage.Txn) server.Context {
				return fakecontext.NewContext()
			})
			defer patch2.Reset()
			_, err = getUploadFunctionCodeHelper(txn, fv, "mock-bucket-id", "mock=obj-id")
			So(err, ShouldNotBeNil)
		})

	})
}

func TestDeleteFunctionByVersion(t *testing.T) {
	Convey("Test deleteFunctionByVersion", t, func() {
		txn := &storage.Txn{}
		Convey("with GetAliasNumByFunctionName err", func() {
			patches := gomonkey.ApplyFunc(storage.GetAliasNumByFunctionName, func(storage.Transaction, string) (int, error) {
				return 0, errors.New("mock err")
			})
			defer patches.Reset()
			err := deleteFunctionByVersion(txn, "mock-func", "mock-ver")
			So(err, ShouldNotBeNil)
		})
		Convey("with AliasRoutingExist err", func() {
			patches := gomonkey.ApplyFunc(storage.GetAliasNumByFunctionName, func(storage.Transaction, string) (int, error) {
				return 0, nil
			}).ApplyFunc(storage.AliasRoutingExist, func(txn storage.Transaction, funcName, funcVer string) (bool, error) {
				return true, errors.New("mock err")
			})
			defer patches.Reset()
			err := deleteFunctionByVersion(txn, "mock-func", "mock-ver")
			So(err, ShouldNotBeNil)
		})
		Convey("with AliasRoutingExist", func() {
			patches := gomonkey.ApplyFunc(storage.GetAliasNumByFunctionName, func(storage.Transaction, string) (int, error) {
				return 0, nil
			}).ApplyFunc(storage.AliasRoutingExist, func(txn storage.Transaction, funcName, funcVer string) (bool, error) {
				return true, nil
			})
			defer patches.Reset()
			err := deleteFunctionByVersion(txn, "mock-func", "mock-ver")
			So(err, ShouldNotBeNil)
		})
		patches := gomonkey.ApplyFunc(storage.GetAliasNumByFunctionName, func(storage.Transaction, string) (int, error) {
			return 0, nil
		}).ApplyFunc(storage.AliasRoutingExist, func(txn storage.Transaction, funcName, funcVer string) (bool, error) {
			return false, nil
		}).ApplyFunc(storage.DeleteFunctionVersion, func(txn storage.Transaction, funcName string, funcVer string) error {
			return nil
		})
		defer patches.Reset()
		Convey("with DeleteFunctionStatus err", func() {
			patch2 := gomonkey.ApplyFunc(storage.DeleteFunctionStatus, func(storage.Transaction, string, string) error {
				return errors.New("mock err")
			})
			defer patch2.Reset()
			err := deleteFunctionByVersion(txn, "mock-func", "mock-ver")
			So(err, ShouldNotBeNil)
		})
		Convey("with DeleteTriggerByFuncNameVersion err", func() {
			patch2 := gomonkey.ApplyFunc(storage.DeleteFunctionStatus, func(storage.Transaction, string, string) error {
				return nil
			}).ApplyFunc(DeleteTriggerByFuncNameVersion, func(storage.Transaction, string, string) error {
				return errors.New("mock err")
			})
			defer patch2.Reset()
			err := deleteFunctionByVersion(txn, "mock-func", "mock-ver")
			So(err, ShouldNotBeNil)
		})
		Convey("with ok", func() {
			patch2 := gomonkey.ApplyFunc(storage.DeleteFunctionStatus, func(storage.Transaction, string, string) error {
				return nil
			}).ApplyFunc(DeleteTriggerByFuncNameVersion, func(storage.Transaction, string, string) error {
				return nil
			})
			defer patch2.Reset()
			err := deleteFunctionByVersion(txn, "mock-func", "mock-ver")
			So(err, ShouldBeNil)
		})
	})
}

func TestDeleteResponseForMeta(t *testing.T) {
	Convey("Test deleteResponseForMeta etcd err", t, func() {
		ctx := fakecontext.NewMockContext()
		txn := storage.GetTxnByKind(ctx, "")
		patches := gomonkey.ApplyFunc(storage.GetFunctionVersions, func(txn storage.Transaction,
			funcName string) ([]storage.FunctionVersionValue, error) {
			return []storage.FunctionVersionValue{{
				FunctionVersion: storage.FunctionVersion{Version: "v1", Environment: ""},
				Function:        storage.Function{Name: "0-test-hello"}}}, nil
		}).ApplyFunc(storage.GetAliasNumByFunctionName,
			func(txn storage.Transaction, funcName string) (int, error) {
				return 0, nil
			}).ApplyFunc(storage.AliasRoutingExist, func(txn storage.Transaction, funcName, funcVer string) (bool, error) {
			return false, nil
		}).ApplyFunc(storage.DeleteFunctionVersion, func(txn storage.Transaction, funcName string,
			funcVer string) error {
			return nil
		}).ApplyFunc(storage.DeleteFunctionStatus, func(txn storage.Transaction, funcName string, funcVer string) error {
			return nil
		}).ApplyFunc(publish.DeletePublishFunction, func(txn storage.Transaction, name string,
			tenantInfo server.TenantInfo, funcVersion string) {
		}).ApplyMethod(reflect.TypeOf(txn), "Commit", func(_ *storage.Txn) error {
			return snerror.New(10000, "etcd transation failed")
		})
		defer patches.Reset()
		_, err := DeleteResponseForMeta(ctx, server.TenantInfo{TenantID: "123456"}, "0-test-hello", "v1", "")
		So(err, ShouldNotBeNil)
	})
}

func TestCheckFunctionLayerList(t *testing.T) {
	Convey("Test checkFunctionLayerList", t, func() {
		txn := &storage.Txn{}
		urns := []string{"a"}
		Convey("when ParseLayerInfo err", func() {
			patches := gomonkey.ApplyFunc(ParseLayerInfo, func(ctx server.Context, s string) (model.LayerQueryInfo, error) {
				return model.LayerQueryInfo{}, errors.New("mock err")
			})
			defer patches.Reset()
			err := checkFunctionLayerList(txn, urns, "go")
			So(err, ShouldNotBeNil)
		})
		Convey("when found", func() {
			patches := gomonkey.ApplyFunc(ParseLayerInfo, func(ctx server.Context, s string) (model.LayerQueryInfo, error) {
				return model.LayerQueryInfo{}, nil
			}).ApplyFunc(storage.GetLayerVersionTx, func(
				txn storage.Transaction, layerName string, layerVersion int) (storage.LayerValue, error) {
				return storage.LayerValue{CompatibleRuntimes: []string{"go"}}, nil
			})
			defer patches.Reset()
			err := checkFunctionLayerList(txn, urns, "go")
			So(err, ShouldBeNil)
		})
		Convey("when not found", func() {
			patches := gomonkey.ApplyFunc(ParseLayerInfo, func(ctx server.Context, s string) (model.LayerQueryInfo, error) {
				return model.LayerQueryInfo{}, nil
			}).ApplyFunc(storage.GetLayerVersionTx, func(
				txn storage.Transaction, layerName string, layerVersion int) (storage.LayerValue, error) {
				return storage.LayerValue{CompatibleRuntimes: []string{"gg"}}, nil
			})
			defer patches.Reset()
			err := checkFunctionLayerList(txn, urns, "go")
			So(err, ShouldNotBeNil)
		})
		Convey("when repetitive", func() {
			patches := gomonkey.ApplyFunc(ParseLayerInfo, func(ctx server.Context, s string) (model.LayerQueryInfo, error) {
				return model.LayerQueryInfo{}, nil
			}).ApplyFunc(storage.GetLayerVersionTx, func(
				txn storage.Transaction, layerName string, layerVersion int) (storage.LayerValue, error) {
				return storage.LayerValue{CompatibleRuntimes: []string{"go"}}, nil
			})
			defer patches.Reset()
			err := checkFunctionLayerList(txn, []string{"a", "a"}, "go")
			So(err, ShouldNotBeNil)
		})
	})
}

func TestCanDelFunctionPackage(t *testing.T) {
	Convey("Test canDelFunctionPackage", t, func() {
		txn := &storage.Txn{}
		Convey("when IsObjectReferred err", func() {
			fvo := storage.FunctionVersionValue{}
			fvo.FunctionVersion.Version = "a"
			fvs := []storage.FunctionVersionValue{fvo}
			patches := gomonkey.ApplyFunc(storage.IsObjectReferred,
				func(txn storage.Transaction, bucketID string, objectID string) (bool, error) {
					return false, errors.New("mock err")
				})
			defer patches.Reset()
			err := canDelFunctionPackage(txn, "a", fvs)
			So(err, ShouldNotBeNil)
		})
		Convey("when referred", func() {
			fvo := storage.FunctionVersionValue{}
			fvo.FunctionVersion.Version = "a"
			fvs := []storage.FunctionVersionValue{fvo}
			patches := gomonkey.ApplyFunc(storage.IsObjectReferred,
				func(txn storage.Transaction, bucketID string, objectID string) (bool, error) {
					return true, nil
				})
			defer patches.Reset()
			err := canDelFunctionPackage(txn, "a", fvs)
			So(err, ShouldBeNil)
		})
		Convey("when AddUncontrolledTx err", func() {
			fvo := storage.FunctionVersionValue{}
			fvo.FunctionVersion.Version = "a"
			fvs := []storage.FunctionVersionValue{fvo}
			patches := gomonkey.ApplyFunc(storage.IsObjectReferred,
				func(txn storage.Transaction, bucketID string, objectID string) (bool, error) {
					return false, nil
				}).ApplyFunc(storage.AddUncontrolledTx, func(txn storage.Transaction, bucketID, objectID string) error {
				return errors.New("mock err")
			})
			defer patches.Reset()
			err := canDelFunctionPackage(txn, "a", fvs)
			So(err, ShouldNotBeNil)
		})
	})
}

func Test_getFunctionByAlias(t *testing.T) {
	Convey("Test getFunctionByAlias", t, func() {
		patches := gomonkey.ApplyFunc(storage.GetFunctionByFunctionNameAndAlias, func(
			c server.Context, funcName string, aliasName string) string {
			return "1"
		}).ApplyFunc(getFunctionByVersion, func(c server.Context, f string, v string, _ string) (model.FunctionGetResponse, error) {
			return model.FunctionGetResponse{}, nil
		})
		defer patches.Reset()
		_, err := getFunctionByAlias(fakecontext.NewMockContext(), "f", "a", "")
		So(err, ShouldBeNil)
	})
}

func TestDeleteTriggerByFuncName(t *testing.T) {
	Convey("Test DeleteTriggerByFuncName", t, func() {
		patches := gomonkey.ApplyFunc(publish.DeleteTriggerByFuncName, func(txn storage.Transaction, funcName string) error {
			return errors.New("mock err")
		})
		defer patches.Reset()
		err := DeleteTriggerByFuncName(&storage.Txn{}, "mock-fun")
		So(err, ShouldNotBeNil)
	})
	Convey("Test DeleteTriggerByFuncName 2", t, func() {
		patches := gomonkey.ApplyFunc(publish.DeleteTriggerByFuncName, func(txn storage.Transaction, funcName string) error {
			return nil
		}).ApplyFunc(storage.DeleteTriggerByFunctionNameVersion, func(txn storage.Transaction, funcName, verOrAlias string) error {
			return errors.New("mock err")
		})
		defer patches.Reset()
		err := DeleteTriggerByFuncName(&storage.Txn{}, "mock-fun")
		So(err, ShouldNotBeNil)
	})
}

func Test_buildUpdateFunctionVersion(t *testing.T) {
	Convey("Test buildUpdateFunctionVersion", t, func() {
		Convey("with json Marshal err", func() {
			patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("mock err")
			})
			defer patch.Reset()
			req := model.FunctionUpdateRequest{}
			fv := &storage.FunctionVersionValue{}
			req.CustomResources = map[string]float64{"a": 0.1}
			err := buildUpdateFunctionVersion(req, fv)
			So(err, ShouldNotBeNil)
		})
		Convey("with utils.EncodeEnv err", func() {
			patch := gomonkey.ApplyFunc(utils.EncodeEnv, func(m map[string]string) (string, error) {
				return "", errors.New("mock err")
			})
			defer patch.Reset()
			req := model.FunctionUpdateRequest{}
			fv := &storage.FunctionVersionValue{}
			req.CustomResources = map[string]float64{"a": 0.1}
			req.Environment = map[string]string{"env1": "a"}
			err := buildUpdateFunctionVersion(req, fv)
			So(err, ShouldNotBeNil)
		})
		Convey("with Atoi ConcurrentNum err", func() {
			patch := gomonkey.ApplyFunc(utils.EncodeEnv, func(m map[string]string) (string, error) {
				return "", nil
			})
			defer patch.Reset()
			req := model.FunctionUpdateRequest{}
			fv := &storage.FunctionVersionValue{}
			req.ConcurrentNum = "abc"
			req.Environment = map[string]string{"env1": "a"}
			err := buildUpdateFunctionVersion(req, fv)
			So(err, ShouldNotBeNil)
		})
		Convey("with strconv.ParseInt err", func() {
			req := model.FunctionUpdateRequest{}
			fv := &storage.FunctionVersionValue{}
			req.MinInstance = "abc"
			err := buildUpdateFunctionVersion(req, fv)
			So(err, ShouldNotBeNil)
		})
		Convey("with strconv.ParseInt err 2", func() {
			req := model.FunctionUpdateRequest{}
			fv := &storage.FunctionVersionValue{}
			req.MaxInstance = "abc"
			err := buildUpdateFunctionVersion(req, fv)
			So(err, ShouldNotBeNil)
		})
		Convey("with updateFunctionLayer err", func() {
			patch := gomonkey.ApplyFunc(updateFunctionLayer, func(request model.FunctionUpdateRequest, fv *storage.FunctionVersionValue) error {
				return errors.New("mock err")
			})
			defer patch.Reset()
			req := model.FunctionUpdateRequest{}
			fv := &storage.FunctionVersionValue{}
			err := buildUpdateFunctionVersion(req, fv)
			So(err, ShouldNotBeNil)
		})
	})
}

func Test_deleteAllFunctions(t *testing.T) {
	Convey("Test deleteAllFunctions", t, func() {
		mode := 0
		patches := gomonkey.ApplyFunc(storage.DeleteFunctionVersions, func(txn storage.Transaction, funcName string) error {
			if mode == 0 {
				return errors.New("mock err")
			}
			return nil
		}).ApplyFunc(storage.DeleteFunctionStatuses, func(txn storage.Transaction, funcName string) error {
			if mode == 1 {
				return errors.New("mock err")
			}
			return nil
		}).ApplyFunc(storage.DeleteAliasByFunctionName, func(txn storage.Transaction, funcName string) error {
			if mode == 2 {
				return errors.New("mock err")
			}
			return nil
		}).ApplyFunc(DeleteTriggerByFuncName, func(txn storage.Transaction, funcName string) error {
			if mode == 3 {
				return errors.New("mock err")
			}
			return nil
		})
		defer patches.Reset()
		for mode = 0; mode < 4; mode++ {
			err := deleteAllFunctions(&storage.Txn{}, "mock-func-name")
			So(err, ShouldNotBeNil)
		}
	})
}

func Test_deletePackage(t *testing.T) {
	Convey("Test deletePackage", t, func() {
		txn := &storage.Txn{}
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		Convey("when txn.Commit err", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(txn), "Commit", func(_ *storage.Txn) error {
				return errors.New("mock err")
			})
			defer patch.Reset()
			err := deletePackage(txn, fakecontext.NewMockContext(), nil)
			So(err, ShouldNotBeNil)
		})
		patches.ApplyMethod(reflect.TypeOf(txn), "Commit", func(_ *storage.Txn) error {
			return nil
		})
		Convey("when deleteFunctionPackage err", func() {
			patch := gomonkey.ApplyFunc(deleteFunctionPackage, func(ctx server.Context, fvs []storage.FunctionVersionValue) error {
				return errors.New("mock err")
			})
			defer patch.Reset()
			err := deletePackage(txn, fakecontext.NewMockContext(), nil)
			So(err, ShouldNotBeNil)
		})
	})
}
