#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const { 
    MACH_WRITE_UINT32(buf,SCHEMA_MAGIC_NUM);//写入魔数
    buf += 4;
    MACH_WRITE_UINT32(buf, GetColumnCount());//写入column的数量
    buf += 4;
    uint32_t Offset = 8; //代表的是魔数和column数量的size
    for (uint32_t i = 0; i < GetColumnCount(); i++) { //写入column的指针
        columns_[i]->SerializeTo(buf);
        buf += columns_[i]->GetSerializedSize();//每次写入一个column的指针，Offset就增加一个column的size
        Offset += columns_[i]->GetSerializedSize();//每次写入一个column的指针，Offset就增加一个column的size
    }
    return Offset;
}

uint32_t Schema::GetSerializedSize() const {
    if (columns_.empty())
    {
        return 0;
    }
    else
    {
        uint32_t Offset = 8;
        for (uint32_t i = 0; i < GetColumnCount(); i++) {
            Offset += columns_[i]->GetSerializedSize();
        }
        return Offset;
    }
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    if(MACH_READ_UINT32(buf) != SCHEMA_MAGIC_NUM)           //读取魔数，如果魔数不是schema对应的数，报错
    {
        LOG(WARNING) << "Wrong magic number in column deserialize." << std::endl;
    }
    buf += 4;
    uint32_t size = MACH_READ_UINT32(buf);//读取column的数量
    buf += 4;
    uint32_t Offset = 8;
    std::vector<Column *> ColumnArray;
    for (uint32_t i = 0; i < size; i++) {
        Column* temp;
        Column::DeserializeFrom(buf,temp); // 读取column指针
        ColumnArray.emplace_back(temp);  // 将column指针放入vector中
        buf += temp->GetSerializedSize(); 
        Offset += temp->GetSerializedSize();
    }
    schema =  new Schema(ColumnArray);
    return Offset;
}