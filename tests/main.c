#include "greatest.h"

extern SUITE(ring_suite);
extern SUITE(osd_suite);
extern SUITE(ctrl_suite);

GREATEST_MAIN_DEFS();

int main(int argc, char **argv)
{
	GREATEST_MAIN_BEGIN();
	RUN_SUITE(ring_suite);
	RUN_SUITE(osd_suite);
	RUN_SUITE(ctrl_suite);
	GREATEST_MAIN_END();
}
