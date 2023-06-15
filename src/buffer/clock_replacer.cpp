#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages)
    :SecondChance(num_pages,State::EMPTY),
      pointer(0),
      capacity(num_pages) {}

CLOCKReplacer::~CLOCKReplacer() {
}

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  size_t NonUmpty = 0, i;
  frame_id_t VictimId = 0;   // the first victim

  for (i = 0; i < capacity; i++) {
    auto id = (pointer + i) % capacity;   // get the current frame id
    if (SecondChance[id] == State::EMPTY)  // if empty
      continue;
    else if (SecondChance[id] == State::ACCESSED) { // if accessed
      NonUmpty++;
      SecondChance[id] = State::UNUSED;  // second chance reset
    } else if (SecondChance[id] == State::UNUSED) { // if unused
      NonUmpty++; // count the nonempty
      // get the first victim
      VictimId = (VictimId != 0) ? VictimId : id;   // get the first victim
    }
  }

  // all empty, return false
  if (NonUmpty == 0) {
    frame_id = nullptr;
    return false;
  }

  if (VictimId == 0) {  // if the first victim is empty
    for (i = 0; i < capacity; i++) {  // find the first nonempty
      auto id = (pointer + i) % capacity;  // get the current frame id
      if (SecondChance[id] == State::UNUSED) {  // if unused
        VictimId = id;    // get the first victim
        break;
      }
    }
  }

  SecondChance[VictimId] = State::EMPTY;  // set the victim to empty
  pointer = VictimId;  // set the pointer to the victim
  *frame_id = VictimId;  // set the frame id

  return true;
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  //remove from replacer
  SecondChance[frame_id % capacity] = State::EMPTY;
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  //add into replacer
  SecondChance[frame_id % capacity] = State::ACCESSED;
}

/**
 * @breif count those State != EMPTY
 * @return the current size of replacer
 */

size_t CLOCKReplacer::Size() {
  return count_if(SecondChance.begin(), SecondChance.end(), IsEmpty);
}

/**
 * @param itr
 * @return if *itr != State::EMPTY, return true, otherwise false
 */

bool CLOCKReplacer::IsEmpty(CLOCKReplacer::State& item) {
  return item != State::EMPTY;
}