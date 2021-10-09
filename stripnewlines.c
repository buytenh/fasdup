#include <stdio.h>
#include <stdlib.h>

int main()
{
	int c;
	int headerline;

	c = fgetc(stdin);
	if (c != '>') {
		fprintf(stderr, "error: expected > as first character\n");
		return 1;
	}

	headerline = 1;
	fputc(c, stdout);

	while ((c = fgetc(stdin)) != EOF) {
		if (c == '\n' && !headerline)
			continue;

		if (c == '\n') {
			headerline = 0;
		} else if (c == '>') {
			headerline = 1;
			fputc('\n', stdout);
		}

		fputc(c, stdout);
	}

	fputc('\n', stdout);

	return 0;
}
