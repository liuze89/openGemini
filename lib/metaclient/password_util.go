// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package metaclient

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	
	"go.uber.org/zap"
	"golang.org/x/crypto/pbkdf2"
)

const (
	pbkdf2Iter4096 = 4096
	pbkdf2Iter1000 = 1000
	pbkdf2KeyLen   = 32
	SaltBytes      = 32
)

type PasswordUtil struct {
	algoVer int
	logger  *zap.Logger
}

func NewPasswordUtil(algoVer int, logger *zap.Logger) *PasswordUtil {
	return &PasswordUtil{
		algoVer: algoVer,
		logger:  logger,
	}
}

// 核心加密方法
func (u *PasswordUtil) EncryptWithSalt(salt []byte, plaintext string) []byte {
	switch u.algoVer {
	case algoVer01:
		return u.hashWithSalt(salt, plaintext)
	case algoVer02, algoVer03:
		return u.pbkdf2WithSalt(salt, plaintext)
	default:
		return nil
	}
}

func (u *PasswordUtil) hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	if _, err := hasher.Write(salt); err != nil {
		u.logger.Error("hash salt write error", zap.Error(err))
		return nil
	}
	if _, err := hasher.Write([]byte(password)); err != nil {
		u.logger.Error("hash password write error", zap.Error(err))
		return nil
	}
	return hasher.Sum(nil)
}

func (u *PasswordUtil) pbkdf2WithSalt(salt []byte, password string) []byte {
	iter := pbkdf2Iter4096
	if u.algoVer == algoVer03 {
		iter = pbkdf2Iter1000
	}
	return pbkdf2.Key([]byte(password), salt, iter, pbkdf2KeyLen, sha256.New)
}

// 生成盐值
func (u *PasswordUtil) GenerateSalt() ([]byte, error) {
	salt := make([]byte, SaltBytes)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		u.logger.Error("generate salt error", zap.Error(err))
		return nil, err
	}
	return salt, nil
}

// 生成完整密码哈希
func (u *PasswordUtil) GenerateHashedPassword(plaintext string) (string, error) {
	switch u.algoVer {
	case algoVer01:
		return u.generateSHA256Password(plaintext)
	case algoVer02, algoVer03:
		return u.generatePBKDF2Password(plaintext)
	default:
		return "", fmt.Errorf("unsupported algorithm version: %d", u.algoVer)
	}
}

// SHA256加密实现
func (u *PasswordUtil) generateSHA256Password(password string) (string, error) {
	salt, hashed, err := u.saltedHash(password)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%02X%02X", hashAlgoVerOne, salt, hashed), nil
}

// PBKDF2加密实现
func (u *PasswordUtil) generatePBKDF2Password(password string) (string, error) {
	salt, hashed, err := u.saltedPbkdf2(password)
	if err != nil {
		return "", err
	}
	
	versionPrefix := hashAlgoVerTwo
	if u.algoVer == algoVer03 {
		versionPrefix = hashAlgoVerThree
	}
	return fmt.Sprintf("%s%02X%02X", versionPrefix, salt, hashed), nil
}

// 密码验证相关方法
func (u *PasswordUtil) CompareHashAndPassword(hashed, plaintext string) error {
	switch {
	case strings.HasPrefix(hashed, hashAlgoVerOne):
		return u.compareSHA256Password(hashed, plaintext)
	case strings.HasPrefix(hashed, hashAlgoVerTwo):
		return u.comparePbkdf2Password(hashed, plaintext, pbkdf2Iter4096)
	case strings.HasPrefix(hashed, hashAlgoVerThree):
		return u.comparePbkdf2Password(hashed, plaintext, pbkdf2Iter1000)
	default:
		return fmt.Errorf("unknown hash algorithm version")
	}
}

// 其他辅助方法保持不变，仅移动到新文件...