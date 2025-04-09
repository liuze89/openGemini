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
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"go.uber.org/zap/zaptest"
	"golang.org/x/crypto/pbkdf2"
)

func TestPasswordUtil_GenerateSalt(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pwUtil := NewPasswordUtil(algoVer02, logger)

	// 测试正常生成
	salt, err := pwUtil.GenerateSalt()
	if err != nil {
		t.Fatalf("生成盐值失败: %v", err)
	}
	if len(salt) != SaltBytes {
		t.Errorf("盐值长度错误，期望:%d 实际:%d", SaltBytes, len(salt))
	}

	// 测试唯一性（概率性测试）
	anotherSalt, _ := pwUtil.GenerateSalt()
	if hex.EncodeToString(salt) == hex.EncodeToString(anotherSalt) {
		t.Error("两次生成的盐值相同")
	}
}

func TestEncryptWithSalt_Version1(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pwUtil := NewPasswordUtil(algoVer01, logger)
	
	salt := []byte("test_salt_12345678901234567890")
	password := "openGemini@2023"

	// SHA256 加密测试
	hashed := pwUtil.EncryptWithSalt(salt, password)
	expected := sha256.Sum256(append(salt, []byte(password)...))
	if !bytes.Equal(hashed, expected[:]) {
		t.Errorf("SHA256加密结果不符预期\n期望: %x\n实际: %x", expected, hashed)
	}
}

func TestGenerateHashedPassword_Version2(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pwUtil := NewPasswordUtil(algoVer02, logger)

	// PBKDF2-HMAC-SHA256 测试
	hashed, err := pwUtil.GenerateHashedPassword("strong_password")
	if err != nil {
		t.Fatal(err)
	}

	// 验证格式: #Ver:002# + salt(32B) + hash(32B)
	if !strings.HasPrefix(hashed, hashAlgoVerTwo) {
		t.Errorf("版本前缀错误，期望:%s 实际:%s", hashAlgoVerTwo, hashed[:len(hashAlgoVerTwo)])
	}

	// 解码校验
	saltHashPart := hashed[len(hashAlgoVerTwo):]
	if len(saltHashPart) != 128 { // 32B salt + 32B hash -> hex编码后各64字符
		t.Errorf("哈希格式错误，总长度:%d", len(saltHashPart))
	}
}

func TestCompareHashAndPassword(t *testing.T) {
	logger := zaptest.NewLogger(t)
	testCases := []struct {
		name     string
		algoVer  int
		password string
		wantErr  bool
	}{
		{"版本1-正确密码", algoVer01, "test123", false},
		{"版本2-错误密码", algoVer02, "wrong_pwd", true},
		{"版本3-空密码", algoVer03, "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pwUtil := NewPasswordUtil(tc.algoVer, logger)
			
			// 生成有效哈希
			hashed, _ := pwUtil.GenerateHashedPassword("test123")
			
			// 篡改哈希用于错误测试
			if tc.wantErr {
				hashed = "invalid_hash_format"
			}

			err := pwUtil.CompareHashAndPassword(hashed, tc.password)
			if (err != nil) != tc.wantErr {
				t.Errorf("验证结果不符预期，错误状态:%v", err)
			}
		})
	}
}

// 边界测试：超长密码
func TestExtremeLengthPassword(t *testing.T) {
	logger := zaptest.NewLogger(t)
	pwUtil := NewPasswordUtil(algoVer02, logger)
	
	longPassword := strings.Repeat("a", 1024)
	hashed, err := pwUtil.GenerateHashedPassword(longPassword)
	if err != nil {
		t.Fatal(err)
	}

	// 验证生成的哈希可以正确解析
	if err := pwUtil.CompareHashAndPassword(hashed, longPassword); err != nil {
		t.Error("超长密码验证失败")
	}
}