# Hoardbase

Hoardbase is sqlite disguised as a NoSql database with an API similar to that of mongodb. There had been many times that I need a single-file embeded NoSql solution and couldn't find any. For my use cases, a good choice should meet the following requirements:

1. It needs to be NoSql. This is convinent when data are dirty, which is common in the data ETL use case. Another benefit enabled by NoSql is less effort in implementing data backward compatibility. Even when a data schema can eventually be defined and a Sql database is desired, prototyping using NoSql is also easier.
2. The database has to be embedable for easy deployment. In many use cases, for example, a standalone desktop app, the end users don't have the skills for setting up and maintaining a database server.
3. The database must be contained in a single file. This will guarantee data integrity and make data migration and backup easier for untrained users. 
4. There should be cross-language support.

I feel that an embedable NoSql is a very common building block that lacks good choices. The cloest one, in my opinion, is ejdb2. However, that project is inactive and its code readability is poor. But what about this project? Sqlite is a solid fundation and has been battle tested. I try to keep my warpper layer simple and well documented, and hence fixable. 

<!-- cargo-sync-readme start -->

A doc comment that applies to the implicit anonymous module of this crate

<!-- cargo-sync-readme end -->