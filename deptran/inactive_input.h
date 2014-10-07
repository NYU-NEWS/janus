//
//#ifndef INPUT_H_
//#define INPUT_H_
//
//
//#include "all.h"
//
//namespace deptran {
//
//
//class Input {
//
//};
//
//class Output {
//
//};
//
//class TxnOutput{
//
//};
//
//class TxnInput {
//public:
//    int txn_type;
//    uint64_t txn_id_;
//    std::map<std::string, std::string> valueMap;
//
//    void putInt(std::string key, int value) {
//        std::string s((char*)&value, sizeof(value));
//        valueMap[key] = s;
//    }
//    void putDouble(std::string key, double value) {
//        std::string s((char*)&value, sizeof(value));
//        valueMap[key] = s;
//    }
//    void putString(std::string key, std::string value) {
//        valueMap[key] = value;
//    }
//
//    int getInt(std::string key) {
//        const char *data = valueMap[key].data();
//        int i =  *(const int*)data;
//        return i;
//    }
//    double getDouble(std::string key) {
//        const char *data = valueMap[key].data();
//        double d =  *(const double*)data;
//        return d;
//    }
//    std::string getString(std::string key) {
//        return valueMap[key];
//    }
//};
//
//class PieceInput : public TxnInput{
//public:
//    int piece_type ;
//    uint64_t pie_id_;
//};
//
//class PieceOutput {
//public:
//    int status_;
//
//};
//
//}
//
//#endif
