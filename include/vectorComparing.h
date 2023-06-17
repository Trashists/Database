#include <vector>

//function for comparing vector
bool are_vectors_equal(std::vector<int>& v1, std::vector<int>& v2) {
    if (v1.size() != v2.size()) { // check if the std::vectors are of the same size
        return false;
    }

    for (int i = 0; i < v1.size(); i++) { // iterate over each element of the vectors
        if (v1[i] != v2[i]) { // check if the corresponding elements of the vectors are not equal
            return false;
        }
    }

    return true; // if all elements are equal, the std::vectors are equal
}
