// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recover

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/openGemini/openGemini/lib/config"
	fileops "github.com/openGemini/openGemini/lib/fileops"
)

const FullAndIncRecoverMode = "1"
const FullRecoverMode = "2"

type RecoverOptions struct {
	RecoverMode        string
	DataDir            string
	MetaDir            string
	FullBackupDataPath string
	IncBackupDataPath  string
	OnlyRecoverMeta    bool
	Force              bool
	// leader meta address
	Host        string
	SSL         bool
	InsecureTLS bool
}

type RecoverFunc func(path string) error

func (r *RecoverOptions) BackupRecover() error {
	if r.FullBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: fullBackupDataPath")
	}
	if r.RecoverMode == "1" && r.IncBackupDataPath == "" {
		return fmt.Errorf("`missing required parameter: incBackupDataPath")
	}
	var err error
	dbs, err := r.getDatabases()
	if err != nil {
		return err
	}
	isInc := r.RecoverMode == FullAndIncRecoverMode
	if r.OnlyRecoverMeta {
		err = r.recoverMeta(isInc, dbs)
	} else {
		err = r.runRecover(isInc, dbs)
	}
	if err != nil {
		return err
	}

	return nil
}

func (r *RecoverOptions) runRecover(isInc bool, dbs []string) error {
	if err := r.recoverData(dbs, isInc); err != nil {
		return err
	}

	return r.recoverMeta(isInc, dbs)
}

func (r *RecoverOptions) recoverData(dbs []string, isInc bool) error {
	dataPath := filepath.Join(r.DataDir, config.DataDirectory)
	// check clear data path
	if len(dbs) > 0 {
		for _, db := range dbs {
			p := filepath.Join(dataPath, db)
			_, err := os.Stat(p)
			if !r.Force && err == nil {
				return fmt.Errorf("target database file exist,db : %s.if you still recover,please use --force", db)
			}
			if err := os.RemoveAll(p); err != nil {
				return err
			}
		}
	} else {
		if err := os.RemoveAll(filepath.Join(dataPath)); err != nil {
			return err
		}
	}

	copyFunc := r.copyWithFull
	if isInc {
		copyFunc = r.copyWithFullAndInc
	}

	// recover full_backup
	fullBackupDataPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir, dataPath)
	if _, err := os.Stat(fullBackupDataPath); err != nil {
		return fmt.Errorf("backupDataPath empty: %s", fullBackupDataPath)
	}
	if err := r.traversalBackupLogFile(fullBackupDataPath, copyFunc, isInc); err != nil {
		return err
	}

	if !isInc {
		return nil
	}

	// recover inc_backup
	incBackupDataPath := filepath.Join(r.IncBackupDataPath, backup.DataBackupDir, dataPath)
	if _, err := os.Stat(incBackupDataPath); err != nil {
		fmt.Println("incBackupDataPath empty !")
		return nil
	}
	if err := r.traversalIncBackupLogFile(incBackupDataPath); err != nil {
		return err
	}

	return nil
}

func (r *RecoverOptions) recoverMeta(isInc bool, dbs []string) error {
	var backupPath string
	if isInc {
		backupPath = r.IncBackupDataPath
	} else {
		backupPath = r.FullBackupDataPath
	}
	backupMetaPath := filepath.Join(backupPath, backup.MetaBackupDir)

	// while single or multi databases backup,then run logic recover
	if len(dbs) > 0 {
		buf, err := os.ReadFile(filepath.Join(backupMetaPath, backup.MetaInfo))
		if err != nil {
			return err
		}
		return r.sendRequestToMeta(dbs, string(buf))
	}

	// while all database backup,then run physics recover
	err := os.RemoveAll(r.MetaDir)
	if err != nil {
		return nil
	}

	var noMeta bool
	if _, err := os.Stat(backupMetaPath); err != nil {
		noMeta = true
	}

	if noMeta {
		return nil
	}

	if err := backup.FolderMove(backupMetaPath, r.MetaDir); err != nil {
		return err
	}
	return nil
}

func (r *RecoverOptions) sendRequestToMeta(dbs []string, metaData string) error {
	protocol := "http"
	if r.SSL {
		protocol = "https"
	}
	urlValues := url.Values{}
	urlValues.Add(backup.DataBases, strings.Join(dbs, ","))
	urlValues.Add(backup.MetaData, metaData)

	Url, err := url.Parse(fmt.Sprintf("%s://%s/recoverMeta", protocol, r.Host))
	if err != nil {
		return err
	}
	Url.RawQuery = urlValues.Encode()

	transport := &http.Transport{}
	client := &http.Client{
		Transport: transport,
	}
	if r.InsecureTLS {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	res, err := client.PostForm(Url.String(), urlValues)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	return resolveResponseError(res)
}

func resolveResponseError(resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("recover meta request error,read response body faild,%s", err.Error())
	}

	return fmt.Errorf("recover meta error,%s", string(b))
}

func (r *RecoverOptions) traversalBackupLogFile(path string, fn RecoverFunc, isInc bool) error {
	var err error
	var fds []fs.FileInfo

	if _, err = os.Stat(path); err != nil {
		return err
	}
	if fds, err = fileops.ReadDir(path); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := filepath.Join(path, fd.Name())

		if fd.IsDir() {
			if fd.Name() == "index" && !isInc {
				outPath := strings.Replace(srcfp, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FolderMove(srcfp, outPath); err != nil {
					return err
				}
				continue
			}
			if err = r.traversalBackupLogFile(srcfp, fn, isInc); err != nil {
				return err
			}
		} else {
			if fd.Name() == backup.FullBackupLog {
				if err := fn(srcfp); err != nil {
					return err
				}
				outPath := strings.Replace(srcfp, filepath.Join(r.FullBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FileMove(srcfp, outPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *RecoverOptions) traversalIncBackupLogFile(path string) error {
	var err error
	var fds []fs.FileInfo

	if _, err = os.Stat(path); err != nil {
		return err
	}
	if fds, err = fileops.ReadDir(path); err != nil {
		return err
	}

	for _, fd := range fds {
		srcfp := filepath.Join(path, fd.Name())

		if fd.IsDir() {
			if fd.Name() == "index" {
				outPath := strings.Replace(srcfp, filepath.Join(r.IncBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FolderMove(srcfp, outPath); err != nil {
					return err
				}
				continue
			}
			if err = r.traversalIncBackupLogFile(srcfp); err != nil {
				return err
			}
		} else {
			if fd.Name() == backup.IncBackupLog {
				if err := r.copyWithInc(srcfp); err != nil {
					return err
				}
				// recover log file
				outPath := strings.Replace(srcfp, filepath.Join(r.IncBackupDataPath, backup.DataBackupDir), "", -1)
				if err := backup.FileMove(srcfp, outPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *RecoverOptions) copyWithFull(path string) error {
	backupLog := &backup.BackupLogInfo{}
	if err := backup.ReadBackupLogFile(path, backupLog); err != nil {
		return err
	}

	basicPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir)
	for _, fileList := range backupLog.FileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *RecoverOptions) copyWithFullAndInc(fullPath string) error {
	p, _ := filepath.Split(strings.Replace(fullPath, r.FullBackupDataPath, r.IncBackupDataPath, -1))
	incPath := filepath.Join(p, backup.IncBackupLog)
	incBackupLog := &backup.IncBackupLogInfo{}
	_, err := fileops.Stat(incPath)
	if err != nil {
		err = r.copyWithFull(fullPath)
		return err
	}
	if err := backup.ReadBackupLogFile(incPath, incBackupLog); err != nil {
		return err
	}

	backupLog := &backup.BackupLogInfo{}
	if err := backup.ReadBackupLogFile(fullPath, backupLog); err != nil {
		return err
	}

	err = r.mergeFileList(backupLog.FileListMap, incBackupLog.DelFileListMap)
	if err != nil {
		return err
	}

	return nil
}

func (r *RecoverOptions) copyWithInc(incPath string) error {
	backupLog := &backup.IncBackupLogInfo{}
	if err := backup.ReadBackupLogFile(incPath, backupLog); err != nil {
		return err
	}

	basicPath := filepath.Join(r.IncBackupDataPath, backup.DataBackupDir)
	for _, fileList := range backupLog.AddFileListMap {
		for _, files := range fileList {
			srcPath := filepath.Join(basicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (r *RecoverOptions) mergeFileList(listMap, delListMap map[string][][]string) error {
	fullBasicPath := filepath.Join(r.FullBackupDataPath, backup.DataBackupDir)
	for name, fileList := range listMap {
		dListSeen := make(map[string]bool)
		if dLists, ok := delListMap[name]; ok {
			for _, files := range dLists {
				dListSeen[files[0]] = true
			}
		}
		for _, files := range fileList {
			// delete
			if dListSeen[files[0]] {
				continue
			}
			srcPath := filepath.Join(fullBasicPath, files[0])
			for _, f := range files {
				if err := backup.FileMove(srcPath, f); err != nil {
					return err
				}
			}
		}

	}
	return nil
}

func (r *RecoverOptions) getDatabases() ([]string, error) {
	path := fileops.Join(r.FullBackupDataPath, backup.BackupLogPath, backup.ResultLog)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	fullRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, fullRes); err != nil {
		return nil, err
	}
	databases := make([]string, 0, len(fullRes.DataBases))

	if r.RecoverMode == FullRecoverMode {
		for db := range fullRes.DataBases {
			databases = append(databases, db)
		}
		return databases, nil
	}

	path = fileops.Join(r.IncBackupDataPath, backup.BackupLogPath, backup.ResultLog)
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	incRes := &backup.BackupResult{}
	if err := backup.ReadBackupLogFile(path, incRes); err != nil {
		return nil, err
	}

	if fullRes.Time > incRes.Time {
		return nil, errors.New("fullBackup time should earlier than incBackup")
	}

	if len(fullRes.DataBases) != len(incRes.DataBases) {
		return nil, errors.New("databases not equal in full Backup and inc Backup")
	}

	for db := range fullRes.DataBases {
		if _, ok := incRes.DataBases[db]; !ok {
			return nil, errors.New("databases not equal in full Backup and inc Backup")
		}
		databases = append(databases, db)
	}

	return databases, nil
}
