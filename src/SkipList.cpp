#include "SkipList.h"
#include <optional>
#include <iostream>

namespace kvaaas {

SkipList::SkipList(const SLBottomLevelRecordViewer &bottom_, const SLUpperLevelRecordViewer &upper_) :
    bottom(bottom_),
    upper(upper_),
    levels_count(1 + upper.get_levels()) {
}

std::uint64_t SkipList::insert_after_on_upper_level(std::uint64_t ind, SLUpperLevelRecord &new_node) {
    new_node.next = upper.get_next(ind);
    std::uint64_t pos = upper.append_record(new_node);
    upper.set_next(ind, pos);
    return pos;
}

std::uint64_t SkipList::insert_as_head_upper_level(std::uint64_t level, SLUpperLevelRecord &new_node) {
    new_node.next = upper.get_head(level);
    std::uint64_t pos = upper.append_record(new_node);
    upper.set_head(level, pos);
    return pos;
}

std::uint64_t SkipList::insert_after_on_bottom_level(std::uint64_t ind, SLBottomLevelRecord &new_node) {
    new_node.next = bottom.get_next(ind);
    std::uint64_t pos = bottom.append_record(new_node);
    bottom.set_next(ind, pos);
    return pos;
}

std::uint64_t SkipList::insert_as_head_bottom_level(SLBottomLevelRecord &new_node) {
    new_node.next = bottom.get_head();
    std::uint64_t pos = bottom.append_record(new_node);
    bottom.set_head(pos);
    return pos;
}

void SkipList::insert_after_parents(const std::vector<std::uint64_t> &parents,
                                    const KeyType &key,
                                    std::uint64_t offset) {
    std::uint64_t down = NULL_NODE;
    auto insert =
            [this, &key, &offset, &down]
            (std::uint64_t level, std::uint64_t after_that) {
        if (level == 0 && after_that == NULL_NODE) {
            SLBottomLevelRecord new_record(key, offset);
            return insert_as_head_bottom_level(new_record);
        }
        if (level == 0 && after_that != NULL_NODE) {
            SLBottomLevelRecord new_record(key, offset);
            return insert_after_on_bottom_level(after_that, new_record);
        }
        if (level != 0 && after_that == NULL_NODE) {
            SLUpperLevelRecord new_record(key, down, offset);
            return insert_as_head_upper_level(level - 1, new_record);
        }
        if (level != 0 && after_that != NULL_NODE) {
            SLUpperLevelRecord new_record(key, down, offset);
            return insert_after_on_upper_level(after_that, new_record);
        }
    };
    std::uint64_t cur_level = 0;
    while (cur_level < parents.size()) {
        down = insert(cur_level, parents[cur_level]);
        ++cur_level;
        if (dist(rng) == 0) {
            break;
        }
    }
    if (cur_level == levels_count && dist(rng) == 1) {
        upper.append_head(NULL_NODE);
        insert(cur_level, NULL_NODE);
        ++levels_count;
    }
}

void SkipList::insert_instead_parents(const std::vector<std::uint64_t> &parents,
                                      const KeyType &key,
                                      std::uint64_t offset) {
    for (std::size_t level = 0; level < parents.size(); ++level) {
        if (parents[level] == NULL_NODE) {
            break;
        }
        if (level == 0) {
            if (bottom[parents[level]].key != key) {
                break;
            }
            bottom.set_offset(parents[level], offset);
        } else {
            if (upper[parents[level]].key != key) {
                break;
            }
            upper.set_offset(parents[level], offset);
        }
    }
}

void SkipList::put(const KeyType &key, std::uint64_t offset) {
    if (!bottom.has_head()) {
        SLBottomLevelRecord new_record(key, offset);
        insert_as_head_bottom_level(new_record);
        return;
    }

    std::vector<std::uint64_t> parents(levels_count, NULL_NODE);
    std::uint64_t bottom_node = NULL_NODE;
    if (levels_count >= 2) {
        std::int64_t upper_level = static_cast<std::int64_t>(levels_count) - 2;
        while (upper_level >= 0 && upper[upper.get_head(upper_level)].key > key) {
            --upper_level;
        }
        if (upper_level != -1) {
            std::uint64_t cur_node = upper.get_head(upper_level);
            while (upper_level >= 0) {
                while (true) {
                    uint64_t next_node = upper.get_next(cur_node);
                    if (next_node != NULL_NODE &&
                        upper[next_node].key <= key) {
                        cur_node = next_node;
                    } else {
                        break;
                    }
                }
                parents[upper_level + 1] = cur_node;
                if (upper_level != 0) {
                    cur_node = upper[cur_node].down;
                }
                --upper_level;
            }
            bottom_node = upper[cur_node].down;
        }
    }
    if (bottom_node == NULL_NODE) {
        bottom_node = bottom.get_head();
    }
    while (true) {
        uint64_t next_node = bottom.get_next(bottom_node);
        if (next_node != NULL_NODE &&
            bottom[next_node].key <= key) {
            bottom_node = next_node;
        } else {
            break;
        }
    }
    if (bottom_node != NULL_NODE && bottom[bottom_node].key == key) {
        parents[0] = bottom_node;
        insert_instead_parents(parents, key, offset);
        return;
    }
    if (bottom_node != NULL_NODE && bottom[bottom_node].key < key) {
        parents[0] = bottom_node;
    }
    insert_after_parents(parents, key, offset);
}


std::optional<std::uint64_t> SkipList::find(const KeyType &key) {
    if (!bottom.has_head()) {
        return {};
    }
    std::uint64_t bottom_node = bottom.get_head();
    if (bottom[bottom_node].key > key) {
        return {};
    }
    if (levels_count >= 2) {
        std::int64_t upper_level = static_cast<std::int64_t>(levels_count) - 2;
        while (upper_level >= 0 && upper[upper.get_head(upper_level)].key > key) {
            --upper_level;
        }
        if (upper_level != -1) {
            std::uint64_t cur_node = upper.get_head(upper_level);
            while (upper_level >= 0) {
                while (true) {
                    uint64_t next_node = upper.get_next(cur_node);
                    if (next_node != NULL_NODE &&
                        upper[next_node].key <= key) {
                        cur_node = next_node;
                    } else {
                        break;
                    }
                }
                if (upper_level != 0) {
                    cur_node = upper[cur_node].down;
                }
                --upper_level;
            }
            bottom_node = upper[cur_node].down;
        }
    }
    while (bottom_node != NULL_NODE &&
          bottom[bottom_node].key < key) {
        bottom_node = bottom.get_next(bottom_node);
    }
    if (bottom[bottom_node].key == key) {
        return bottom[bottom_node].offset;
    } else {
        return {};
    }
}

bool SkipList::has_key(const KeyType &key) {
    return find(key).has_value();
}

}