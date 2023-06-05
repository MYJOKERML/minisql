#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const 
{
    uint32_t Offset = 18 + name_.size() + sizeof(TypeId);   //代表的是魔数+name长度+name+TypeId+数据最大长度+table_ind+nullable+unique

    MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
    buf += 4;                                               //写入魔数，指针向前推进4位

    MACH_WRITE_UINT32(buf, name_.size());                   //写入column对应的name长度，方便读出
    buf += 4;                                               //指针向前推进4位

    MACH_WRITE_STRING(buf, name_);                          //写入column对应的name
    buf += name_.length();                                  //指针向前推进name长度位

    MACH_WRITE_TO(TypeId, buf, type_);                      //写入column对应的type_id（属性对应填入值的type）
    buf += sizeof(TypeId);

    MACH_WRITE_UINT32(buf, len_);                           //数据的最大长度
    buf += 4;

    MACH_WRITE_UINT32(buf, table_ind_);                     //此column在table中的位置
    buf += 4;

    MACH_WRITE_TO(bool, buf, nullable_);                    //该值是否可以为空
    buf++;

    MACH_WRITE_TO(bool, buf, unique_);                      //该列对应的值是否unique
    buf++;

    return Offset;                                          //返回写入的长度
}


/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const 
{
  return 18 + name_.size() + sizeof(TypeId);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) 
{
    if (column != nullptr)                                  //如果column不为空，说明已经有column了，报错
    {
        LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
    }
    
    if(MACH_READ_UINT32(buf) != COLUMN_MAGIC_NUM)           //读取魔数，如果魔数不是Column对应的数，报错
    {
        LOG(WARNING) << "Wrong magic number in column deserialize." 												 << std::endl;
    }
    buf += 4;                                               //指针向前推进4位
    uint32_t NameSize = MACH_READ_UINT32(buf);              //读取name长度
    buf += 4;                                               //指针向前推进4位
    char *name = new char[NameSize];
    for (uint32_t i = 0; i < NameSize; i++)                 //将name一个字节一个字节的读出来
    {
        name[i] = MACH_READ_FROM(char, buf);
        buf++;
    }
    name[NameSize] = 0;                                     // 在最后加上一个'/0'，表示字符串结束
    std::string column_name(name);                          //将char*转换为string
    // read type
    TypeId type = MACH_READ_FROM(TypeId, buf);              //读取type_id
    buf += sizeof(TypeId);                                  //指针向前推进sizeof(TypeId)
    // read len_
    uint32_t col_len = MACH_READ_UINT32(buf);                     //读取数据最大长度
    buf += 4;
    // read table_ind
    uint32_t col_ind = MACH_READ_UINT32(buf);               //读取该column在table中的位置
    buf += 4;
    // read nullable. bool and char are both 1 byte, so we can just read one char
    bool nullable = MACH_READ_FROM(bool, buf);              //读取该值是否可以为空
    buf++;
    // read unique. bool and char are both 1 byte, so we can just read one char
    bool unique = MACH_READ_FROM(bool, buf);                //读取该列对应的值是否unique
    buf++;
    uint32_t Offset = 18 + NameSize + sizeof(TypeId);       //计算读取的长度
    // 将新生成的对象放到heap中
    if (type == kTypeChar) {
        column = new Column(name, type, col_len, col_ind, nullable, unique);
    } else {
        column = new Column(name, type, col_ind, nullable, unique);
    }
    // return Offset
    return Offset;
}
