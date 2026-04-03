# Issues
1. Есть проблема как мне кажется, если партиции не будет по причине неработающего PgTableManager, то её никто не создаст. 
Нужно придумать как это делать. 

## Notes

- Verified with `mvn -q -pl core test`.
- Verified with `mvn -q -pl integration-tests -am test`.
