#include "SkipList.h"

namespace kvaaas {

SkipList::SkipList(const SLBottomLevelRecordViewer &bottom_, const SLUpperLevelRecordViewer &upper_) :
    bottom(bottom_),
    upper(upper_),
    levels_count(1 + upper.get_levels()) {
}

void SkipList::add(KeyType key, std::uint64_t offset) {
    SLUpperLevelRecord node;
    if (levels_count >= 2) {
        SLUpperLevelRecord cur_node = upper.get_record(upper.get_head(levels_count - 2));
        for (std::uint64_t current_level = levels_count - 2; current_level != std::uint64_t(-1); --current_level) {
            while (cur_node.key < key) {
                cur_node = upper.get_record(cur_node.next);
            }
            if (current_level != 0) {
                cur_node = upper.get_record(cur_node.down);
            }
        }
        node = bottom.get_record(cur_node.down);
    }
}

}