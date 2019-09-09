#include <stdio.h>
#include <stdlib.h>
#include "header.h"

int main()
{
	state_t start_state;
	set_state(&start_state, 0, 0);//initial state
	state_t goal_state;
	set_state(&goal_state, 2, 0);//final state
	list_t l;
	init_list(&l);
	state_t temp;
	int soln = 0;
	add_at_end(&l, &start_state);
	int index;
	
	FILE*fp=fopen("a.txt", "w");
	
	//all possible ways to transfer water from one jug to another
	void (*move[])(const state_t *src, state_t *dst) = {
		pour_in4,
		pour_in3,
		pour_out4,
		pour_out3,
		pour_3to4,
		pour_4to3
	};
	int count=0;
	
	//produces all possible ways to go from the initial state to the final state along with the paths.
	while(1)
	{
		while(l.tail!=NULL && !soln)
		{
			index = l.tail->st.fn_index;
			move[index](&l.tail->st , &temp);
			if(! is_repeated(&l, &temp))
			{
				add_at_end(&l, &temp);
				soln = are_same(&temp, &goal_state);
			}
			else
			{
				while(++l.tail->st.fn_index == 6)
				{
					remove_at_end(&l);
					if(l.tail==NULL)
					{
						break;
					}
				}
			
			}
		}
		if(l.tail==NULL)
		{
			break;
		}
		count=count+1;
		disp_list(&l);
		fp=fopen("a.txt", "a");
		fprintf(fp,"%s","\n");
		soln=0;
	}
	fp=fopen("a.txt", "a");
	fprintf(fp,"%d",count);
	
}
