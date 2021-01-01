package breezedb

const (
	TYPE_INTEGER = 0
	TYPE_VARCHAR = 1
)

type FieldInfo struct {
	filedType int
	length int
}

type Schema struct {
	fields []string
	info map[string]FieldInfo
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

func (this *Schema) AddField(fieldName string, filedType int, length int)  {
	this.fields = append(this.fields, fieldName)
	this.info[fieldName] = FieldInfo{
		filedType: filedType,
		length:    length,
	}
}

func (this *Schema) AddIntegerField(fieldName string)  {
	this.AddField(fieldName, TYPE_INTEGER, 0)
}

func (this *Schema) AddVarcharField(fieldName string, length int)  {
	this.AddField(fieldName, TYPE_VARCHAR, length)
}

func (this *Schema) HasField(fieldName string) bool {
	if _, ok := this.info[fieldName]; ok {
		return true
	}
	return false
}

func (this *Schema) Type(fieldName string) int {
	return this.info[fieldName].filedType
}

func (this *Schema) Length(fieldName string) int {
	switch this.Type(fieldName) {
	case TYPE_INTEGER:
		return 4
	case TYPE_VARCHAR:
		return this.info[fieldName].length
	}
	return 0
}

type Layout struct {
	schema *Schema
	offsets map[string]int
	slotSize int
}

func NewLayout(schema *Schema) *Layout {
	offsets := make(map[string]int)
	pos := 1 // 1 byte for empty/inuse flag
	for _, field := range schema.fields {
		offsets[field] = pos
		pos += schema.Length(field)
	}
	slotSize := pos
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (this *Layout) Schema() *Schema {
	return this.schema
}

func (this *Layout) Offset(fieldName string) int {
	return this.offsets[fieldName]
}

func (this *Layout) SlotSize() int {
	return this.slotSize
}

type RecordPage struct {
	block *Block
	layout *Layout
}

func NewRecordPage(block *Block, layout *Layout) *RecordPage {
	return &RecordPage{
		block:  block,
		layout: layout,
	}
}

func (this *RecordPage) offset(slot int) int {
	return slot * this.layout.SlotSize()
}

func (this *RecordPage) GetInteger(slot int, fieldName string)  {
	//pos := this.offset(slot) + this.layout.Offset(fieldName)
}