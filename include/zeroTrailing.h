#include <cstring>

//trailing zero function
std::string removeTrailingZeros(std::string byteString) {
    size_t lastNonZeroIndex = byteString.size() - 1;
    while (lastNonZeroIndex > 0 && byteString[lastNonZeroIndex] == '\x00') {
        lastNonZeroIndex--;
    }

    return byteString.substr(0, lastNonZeroIndex + 1);
}