// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
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

package influxql

import (
	"bytes"
	"sort"
)

const (
	CreateResource = "CREATE RESOURCE"
	ShowResources  = "SHOW RESOURCES"
	ShowResource   = "SHOW RESOURCE"
	DropResource   = "DROP RESOURCE"
	AlterResource  = "ALTER RESOURCE"
	Properties     = "PROPERTIES"
	WhiteSpace     = " "
)

func (f *Field) WriteDigest(buf *bytes.Buffer) {
	switch item := f.Expr.(type) {
	case *VarRef:
		item.WriteDigest(buf)
	default:
		buf.WriteString(item.String())
	}
	buf.WriteString(" AS ")
	buf.WriteString(f.Alias)
}

func (f *Field) Equal(other *Field) bool {
	if f == nil && other == nil {
		return true
	}
	if (f == nil && other != nil) || (f != nil && other == nil) {
		return false
	}

	if f.Alias != other.Alias {
		return false
	}

	switch e1 := f.Expr.(type) {
	case *VarRef:
		e2, ok := other.Expr.(*VarRef)
		return ok && e1.Equal(e2)
	default:
		return f.String() == other.String()
	}
}

func (r *VarRef) WriteDigest(buf *bytes.Buffer) {
	buf.WriteString(r.Val)
	if r.Type == Unknown {
		return
	}
	buf.WriteString("::")
	buf.WriteString(r.Type.String())
}

func (r *VarRef) Equal(other *VarRef) bool {
	if r == nil && other == nil {
		return true
	}
	if r != nil && other != nil {
		return r.Val == other.Val && r.Alias == other.Alias && r.Type == other.Type
	}
	return false
}

func (a Fields) WriteDigest(buf *bytes.Buffer) {
	for _, f := range a {
		f.WriteDigest(buf)
		buf.WriteByte(',')
	}
}

// WriteDigest returns a string representation of the call.
func (c *Call) WriteDigest(b *bytes.Buffer) {
	b.WriteString(c.Name)
	b.WriteString("(")

	for i, arg := range c.Args {
		if i > 0 {
			b.WriteString(",")
		}
		switch item := arg.(type) {
		case *VarRef:
			item.WriteDigest(b)
		default:
			b.WriteString(item.String())
		}
	}
	b.WriteString(")")
}

// CreateResourceStmt represents a CREATE RESOURCE statement AST
type CreateResourceStmt struct {
	Name       string            // Name of the LLM resource
	Properties map[string]string // User-provided properties
}

func (s *CreateResourceStmt) Depth() int {
	if s != nil {
		return 1 + len(s.Properties)
	}
	return 0
}

func (s *CreateResourceStmt) UpdateDepthForTests() int {
	if s != nil {
		return 1 + len(s.Properties)
	}
	return 0
}

func (s *CreateResourceStmt) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *CreateResourceStmt) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	_, _ = buf.WriteString(CreateResource)
	_, _ = buf.WriteString(WhiteSpace)
	if s.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Name))
		_, _ = buf.WriteString(WhiteSpace)
		_, _ = buf.WriteString(Properties)
		_, _ = buf.WriteString(WhiteSpace)
	}
	sortKeys := make([]string, 0, len(s.Properties))
	for key := range s.Properties {
		sortKeys = append(sortKeys, key)
	}
	sort.Strings(sortKeys)

	_, _ = buf.WriteString("(")
	for i, key := range sortKeys {
		value := s.Properties[key]
		_, _ = buf.WriteString(QuoteIdent(key))
		_, _ = buf.WriteString("=")
		_, _ = buf.WriteString(QuoteIdent(value))
		if i != len(sortKeys)-1 {
			_, _ = buf.WriteString(",")
			_, _ = buf.WriteString(WhiteSpace)
		}
	}
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *CreateResourceStmt) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (*CreateResourceStmt) node() {}

func (*CreateResourceStmt) stmt() {}

// ShowResourceStmt represents a SHOW RESOURCE statement AST
type ShowResourceStmt struct {
	Name string // Name of the resource to display
}

func (s *ShowResourceStmt) Depth() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowResourceStmt) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowResourceStmt) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowResourceStmt) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	_, _ = buf.WriteString(ShowResource)
	_, _ = buf.WriteString(WhiteSpace)
	if s.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Name))
	}
	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *ShowResourceStmt) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Name, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

func (*ShowResourceStmt) node() {}

func (*ShowResourceStmt) stmt() {}

// ShowResourcesStmt represents a SHOW RESOURCES statement AST
type ShowResourcesStmt struct {
}

func (s *ShowResourcesStmt) Depth() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowResourcesStmt) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowResourcesStmt) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowResourcesStmt) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	_, _ = buf.WriteString(ShowResources)
	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *ShowResourcesStmt) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: ReadPrivilege}}, nil
}

func (*ShowResourcesStmt) node() {}

func (*ShowResourcesStmt) stmt() {}

// AlterResourceStmt represents an ALTER RESOURCE statement AST
type AlterResourceStmt struct {
	Name       string            // Name of the resource to modify
	Properties map[string]string // Properties to update (partial)
}

func (s *AlterResourceStmt) Depth() int {
	if s != nil {
		return 1 + len(s.Properties)
	}
	return 0
}

func (s *AlterResourceStmt) UpdateDepthForTests() int {
	if s != nil {
		return 1 + len(s.Properties)
	}
	return 0
}

func (s *AlterResourceStmt) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *AlterResourceStmt) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	_, _ = buf.WriteString(AlterResource)
	_, _ = buf.WriteString(WhiteSpace)
	if s.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Name))
		_, _ = buf.WriteString(WhiteSpace)
		_, _ = buf.WriteString(Properties)
		_, _ = buf.WriteString(WhiteSpace)
	}
	sortKeys := make([]string, 0, len(s.Properties))
	for key := range s.Properties {
		sortKeys = append(sortKeys, key)
	}
	sort.Strings(sortKeys)

	_, _ = buf.WriteString("(")
	for i, key := range sortKeys {
		value := s.Properties[key]
		_, _ = buf.WriteString(QuoteIdent(key))
		_, _ = buf.WriteString("=")
		_, _ = buf.WriteString(QuoteIdent(value))
		if i != len(sortKeys)-1 {
			_, _ = buf.WriteString(",")
			_, _ = buf.WriteString(WhiteSpace)
		}
	}
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *AlterResourceStmt) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (*AlterResourceStmt) node() {}

func (*AlterResourceStmt) stmt() {}

// DropResourceStmt represents a DROP RESOURCE statement AST
type DropResourceStmt struct {
	Name string // Name of the resource to drop
}

func (s *DropResourceStmt) Depth() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *DropResourceStmt) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *DropResourceStmt) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *DropResourceStmt) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	_, _ = buf.WriteString(DropResource)
	_, _ = buf.WriteString(WhiteSpace)
	if s.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Name))
	}
	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *DropResourceStmt) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (*DropResourceStmt) node() {}

func (*DropResourceStmt) stmt() {}
