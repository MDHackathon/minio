/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package c14

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apex/log"

	"github.com/dustin/go-humanize"
	"github.com/minio/cli"

	"github.com/juju/errors"

	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/hash"
	"github.com/online-net/c14-cli/pkg/api"
	"github.com/online-net/c14-cli/pkg/client"
	"github.com/online-net/c14-cli/pkg/utils/ssh"
)

const (
	c14Backend = "c14"
)

func init() {
	const c14GatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [ENDPOINT]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
ENDPOINT:
  S3 server endpoint. Default ENDPOINT is https://s3.amazonaws.com

ENVIRONMENT VARIABLES:
  ACCESS:
     MINIO_ACCESS_KEY: Username or access key of S3 storage.
     MINIO_SECRET_KEY: Password or secret key of S3 storage.

  BROWSER:
     MINIO_BROWSER: To disable web browser access, set this value to "off".

  DOMAIN:
     MINIO_DOMAIN: To enable virtual-host-style requests, set this value to Minio host domain name.

  CACHE:
     MINIO_CACHE_DRIVES: List of mounted drives or directories delimited by ";".
     MINIO_CACHE_EXCLUDE: List of cache exclusion patterns delimited by ";".
     MINIO_CACHE_EXPIRY: Cache expiry duration in days.

EXAMPLES:
  1. Start minio gateway server for AWS S3 backend.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ {{.HelpName}}

  2. Start minio gateway server for S3 backend on custom endpoint.
     $ export MINIO_ACCESS_KEY=Q3AM3UQ867SPQQA43P2F
     $ export MINIO_SECRET_KEY=zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG
     $ {{.HelpName}} https://play.minio.io:9000

  3. Start minio gateway server for AWS S3 backend with edge caching enabled.
     $ export MINIO_ACCESS_KEY=accesskey
     $ export MINIO_SECRET_KEY=secretkey
     $ export MINIO_CACHE_DRIVES="/mnt/drive1;/mnt/drive2;/mnt/drive3;/mnt/drive4"
     $ export MINIO_CACHE_EXCLUDE="bucket1/*;*.png"
     $ export MINIO_CACHE_EXPIRY=40
     $ {{.HelpName}}
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               c14Backend,
		Usage:              "c14 online backend for cold storage",
		Action:             c14GatewayMain,
		CustomHelpTemplate: c14GatewayTemplate,
		HideHelpCommand:    true,
	})
}

// Handler for 'minio gateway s3' command line.
func c14GatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	host := ctx.Args().First()
	// Validate gateway arguments.
	logger.FatalIf(minio.ValidateGatewayArguments(ctx.GlobalString("address"), host), "Invalid argument")

	minio.StartGateway(ctx, &c14{})
}

type c14 struct {
	*api.OnlineAPI
}

type uploadFile struct {
	FileFD *os.File
	Info   os.FileInfo
	Path   string
	Name   string
}

func C14Client() (*api.OnlineAPI, error) {
	return client.InitAPI()
}

// Name implements Gateway interface.
func (g *c14) Name() string {
	return c14Backend
}

// NewGatewayLayer returns s3 ObjectLayer.
func (g *c14) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {

	client, err := C14Client()
	if err != nil {
		return nil, err
	}

	return &c14Objects{
		Client: client,
	}, nil
}

// Production - s3 gateway is production ready.
func (g *c14) Production() bool {
	return true
}

type c14Objects struct {
	minio.GatewayUnsupported
	Client *api.OnlineAPI
}

// Shutdown saves any gateway metadata to disk
// if necessary and reload upon next restart.
func (l *c14Objects) Shutdown(ctx context.Context) error {
	return nil
}

// StorageInfo is not relevant to S3 backend.
func (l *c14Objects) StorageInfo(ctx context.Context) (si minio.StorageInfo) {
	return si
}

// MakeBucket creates a new container on S3 backend.
func (l *c14Objects) MakeBucketWithLocation(ctx context.Context, bucket, location string) (err error) {
	var (
		uuidArchive string
		safeName    string
		keys        []api.OnlineGetSSHKey
		crypto      string
		UuidSshKeys []string
	)

	if keys, err = l.Client.GetSSHKeys(); err != nil {
		err = errors.Annotate(err, "Run:GetSSHKey")
		return
	}
	if len(keys) == 0 {
		err = errors.New("Please add an SSH Key here: https://console.online.net/en/account/ssh-keys")
		return
	}
	UuidSshKeys = append(UuidSshKeys, keys[0].UUIDRef)
	safeName = fmt.Sprintf("%s_safe", bucket)
	crypto = "aes-256-cbc"

	if _, uuidArchive, _, err = l.Client.CreateSSHBucketFromScratch(api.ConfigCreateSSHBucketFromScratch{
		SafeName:    safeName,
		ArchiveName: bucket,
		Desc:        "an other bucket",
		UUIDSSHKeys: UuidSshKeys,
		Platforms:   []string{"1"},
		Days:        7,
		Quiet:       false,
		Parity:      "standard",
		LargeBucket: true,
		Crypto:      crypto,
	}); err != nil {
		err = errors.Annotate(err, "Run:CreateSSHBucketFromScratch")
		return
	}
	fmt.Printf("%s\n", uuidArchive)
	return
}

// GetBucketInfo gets bucket metadata..
func (l *c14Objects) GetBucketInfo(ctx context.Context, archive string) (bi minio.BucketInfo, err error) {
	var (
		safe   api.OnlineGetSafe
		bucket api.OnlineGetBucket
	)

	l.Client.FetchRessources()
	if safe, _, err = l.Client.FindSafeUUIDFromArchive(archive, true); err != nil {
		return
	}
	if bucket, err = l.Client.GetBucket(safe.UUIDRef, archive); err != nil {
		return
	}
	fmt.Println("3333333333333333333333")
	fmt.Println("3333333333333333333333")
	fmt.Println(bucket)
	fmt.Println("3333333333333333333333")
	fmt.Println("3333333333333333333333")
	bi = minio.BucketInfo{
		Name: archive,
		// TODO get the real creationDate
		Created: time.Now(),
	}
	return
}

// ListBuckets lists all S3 buckets
func (l *c14Objects) ListBuckets(ctx context.Context) (bi []minio.BucketInfo, err error) {
	var (
		buckets api.OnlineGetArchives
	)
	buckets, err = l.Client.GetAllArchives()
	if err != nil {
		return
	}
	fmt.Println("###########################")
	bi = make([]minio.BucketInfo, buckets.Len())
	for i, b := range buckets {
		fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$$")
		fmt.Println("INDEX ", i)
		fmt.Println("Bucket ", b.CreationDate)
		fmt.Println("$$$$$$$$$$$$$$$$$$$$$$$$$")
		t, _ := time.Parse(time.RFC3339, b.CreationDate)
		bi[i] = minio.BucketInfo{
			Name:    b.Name,
			Created: t,
		}
	}
	fmt.Println("###########################")
	return
}

// DeleteBucket deletes a bucket on S3
func (r *c14Objects) DeleteBucket(ctx context.Context, bucket string) (err error) {
	fmt.Println("inside delete !!!")
	var (
		safe        api.OnlineGetSafe
		uuidArchive string
	)

	if safe, uuidArchive, err = r.Client.FindSafeUUIDFromArchive(bucket, true); err != nil {
		fmt.Println("call 1")
		return
	}
	if err = r.Client.DeleteArchive(safe.UUIDRef, uuidArchive); err != nil {
		fmt.Println("call 2", err)
		return
	}
	if err = r.Client.DeleteSafe(safe.UUIDRef); err != nil {
		fmt.Println("call 3")
		return
	}
	fmt.Println("call 4")
	return
}

// ListObjects lists	 all blobs in S3 bucket filtered by prefix
func (l *c14Objects) ListObjects(ctx context.Context, archive string, prefix string, marker string, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	fmt.Println("Inside list object")

	var (
		sftpCred sshUtils.Credentials
	)

	if sftpCred, err = client.GetsftpCred(l.Client, archive); err != nil {
		return
	}

	fmt.Println("Cred initialized")

	sftpConn, e := client.GetsftpConn(sftpCred)
	if e != nil {
		err = e
		return
	}
	defer sftpCred.Close()
	defer sftpConn.Close()

	if _, _, err = l.Client.FindSafeUUIDFromArchive(archive, true); err != nil {
		if _, _, err = l.Client.FindSafeUUIDFromArchive(archive, false); err != nil {
			return
		}
	}
	loi.IsTruncated = false
	walker := sftpConn.Walk("/buffer")
	for walker.Step() {
		if err = walker.Err(); err != nil {
			log.Debugf("%s", err)
			continue
		}
		if len(walker.Path()) <= 8 {
			continue
		}
		fmt.Println("name : ", walker.Path()[8:])
		fmt.Println("size : ", humanize.Bytes(uint64(walker.Stat().Size())))

		loi.Objects = append(loi.Objects, minio.ObjectInfo{
			Bucket: archive,
			Name:   walker.Path()[8:],
			Size:   walker.Stat().Size(),
		})
	}
	return
}

// ListObjectsV2 lists all blobs in S3 bucket filtered by prefix
func (l *c14Objects) ListObjectsV2(ctx context.Context, archive, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	return loi, minio.ErrorRespToObjectError(err, archive)
}

// GetObject reads an object from S3. Supports additional
// parameters like offset and length which are synonymous with
// HTTP Range requests.
//
// startOffset indicates the starting read location of the object.
// length indicates the total length of the object.
func (l *c14Objects) GetObject(ctx context.Context, bucket string, key string, startOffset int64, length int64, writer io.Writer, etag string) error {
	return nil
}

// GetObjectInfo reads object info and replies back ObjectInfo
func (l *c14Objects) GetObjectInfo(ctx context.Context, bucket string, object string) (objInfo minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, bucket, object)
}

// PutObject creates a new object with the incoming data,
func (l *c14Objects) PutObject(ctx context.Context, archive string, object string, data *hash.Reader, metadata map[string]string) (objInfo minio.ObjectInfo, err error) {

	var (
		sftpCred sshUtils.Credentials
		padding  int
	)

	if sftpCred, err = client.GetsftpCred(l.Client, archive); err != nil {
		return
	}

	sftpConn, e := client.GetsftpConn(sftpCred)
	if e != nil {
		err = e
		return
	}
	defer sftpCred.Close()
	defer sftpConn.Close()

	if err = client.UploadAFile(sftpConn, data, object, data.Size(), padding); err != nil {
		log.Warnf("upload %s: %s", object, err)
	}
	err = nil
	return
}

// CopyObject copies an object from source bucket to a destination bucket.
func (l *c14Objects) CopyObject(ctx context.Context, srcBucket string, srcObject string, dstBucket string, dstObject string, srcInfo minio.ObjectInfo) (objInfo minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, minio.ErrorRespToObjectError(err, srcBucket, srcObject)
}

// DeleteObject deletes a blob in bucket
func (l *c14Objects) DeleteObject(ctx context.Context, bucket string, object string) error {
	// TODO implement
	return nil
}
