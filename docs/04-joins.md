


How to to optimize spark joins ?

- Filter before join if possible, decrease the size of the data to be joined
- Broadcast join if right table is small
- prefer bucketed tables for joins to avoid shuffle

