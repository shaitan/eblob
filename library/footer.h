#ifndef __EBLOB_LIBRARY_FOOTER_H
#define __EBLOB_LIBRARY_FOOTER_H

#include "eblob/blob.h"


#ifdef __cplusplus
extern "C" {
#endif

uint64_t eblob_calculate_footer_size(struct eblob_backend *b, uint64_t data_size);
int eblob_commit_footer(struct eblob_backend *b, struct eblob_key *key, struct eblob_write_control *wc);

#ifdef __cplusplus
}
#endif

#endif /* __EBLOB_LIBRARY_FOOTER_H */
