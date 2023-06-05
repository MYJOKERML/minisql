#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    if (fields_.size() == 0)  // 如果是空行，直接返回0
    {
        return 0;
    }
    uint32_t Offset = 0;
    uint32_t fields_num = fields_.size();
    MACH_WRITE_UINT32(buf, fields_num);  // 写入fields的数量
    Offset += 4;
    buf += 4;
    uint32_t size = (uint32_t)ceil((double)fields_.size() / 8) * 8;  // 向上取整到8的倍数
    uint32_t map_cnt = 0;                                            // map_cnt代表bitmap中的第几个元素
    uint32_t map[size];
    while (map_cnt < size / 8)  // 每次循环写入一个bitmap
    {
        char bitmap = 0;
        for (uint32_t i = 0; i < 8; i++)  // 每次循环写入一个bitmap中的一个bit
        {
        if ((map_cnt * 8 + i < fields_.size()) && (fields_.at(map_cnt * 8 + i)->IsNull() == false))  // 如果不为空，写入1
        {
            bitmap = bitmap | (0x01 << (7 - i));
            map[map_cnt * 8 + i] = 1;  // map中对应的位置也写入1
        } else
            map[map_cnt * 8 + i] = 0;  // 如果为空，写入0
        }
        map_cnt++;                         // map_cnt向后推进一位
        MACH_WRITE_TO(char, buf, bitmap);  // 写入bitmap
        Offset++;
        buf++;
    }
    for (uint32_t i = 0; i < fields_.size(); i++)  // 写入fields
    {
        if (map[i])  // 如果不为空
        {
        uint32_t temp = fields_.at(i)->SerializeTo(buf);  // 写入fields
        buf += temp;
        Offset += temp;
        }
    }
    return Offset;
}
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    uint32_t Offset = 0;
    uint32_t field_num = 0;
    field_num = MACH_READ_UINT32(buf);  // 读取fields的数量
    buf += 4;
    Offset += 4;
    uint32_t size = (uint32_t)ceil((double)field_num / 8) * 8;  // 向上取整到8的倍数
    uint32_t map_cnt = 0;
    uint32_t map[size];
    while (map_cnt < size / 8)  // 每次循环读取一个bitmap
    {
        char bitmap = MACH_READ_FROM(char, buf);
        buf++;
        Offset++;
        for (uint32_t i = 0; i < 8; i++)  // 每次循环读取一个bitmap中的一个bit
        {
        if (((bitmap >> (7 - i)) & 0x01) != 0)  // 如果不为空，写入1
        {
            map[i + map_cnt * 8] = 1;  // map中对应的位置也写入1
        } else {
            map[i + map_cnt * 8] = 0;  // 如果为空，写入0
        }
        }
        map_cnt++;  // map_cnt向后推进一位
    }

    for (uint32_t i = 0; i < field_num; i++) 
    {
        TypeId type = schema->GetColumn(i)->GetType(); 
        uint32_t temp;
        Field *Fieldptr;
        if (type == TypeId::kTypeInt) // 如果是int
        {
            Fieldptr = new Field(TypeId::kTypeInt, 0);
        } 
        else if (type == TypeId::kTypeChar)  // 如果是char
        {
            Fieldptr = new Field(TypeId::kTypeChar, const_cast<char *>(""), strlen(const_cast<char *>("")), false);     
        } 
        else if (type == TypeId::kTypeFloat) // 如果是float
        {
            Fieldptr = new Field(TypeId::kTypeFloat, 0.0f); 
        }
        if (map[i] == 0) // 如果为isnull
        {
            temp = Fieldptr->DeserializeFrom(buf, type, &Fieldptr, true);  
            Offset += temp; // Offset向后推进temp位
            buf += temp;  // buf向后推进temp位
        } 
        else // 如果不为空
        {
            temp = Fieldptr->DeserializeFrom(buf, type, &Fieldptr, false);
            Offset += temp;   
            buf += temp; 
        }
        fields_.push_back(Fieldptr); // 将Fieldptr加入fields中
    }
    return Offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    if (fields_.size() == 0) {// 如果是空行，直接返回0
        return 0;
    }
    uint32_t Offset = 4 + (uint32_t)ceil((double)fields_.size() / 8); // 代表的是fields的数量的大小+bitmap的大小
    // fields
    for (uint32_t i = 0; i < fields_.size(); i++) { // 遍历fields
        if (!fields_.at(i)->IsNull()) {
            Offset += fields_.at(i)->GetSerializedSize(); // 如果不为空，Offset向后推进fields的大小
        }
    }
    return Offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
