import {Global, Module} from '@nestjs/common';
import * as mysql from 'mysql2/promise';

@Global()
@Module({
  providers: [
    {
      provide: 'MYSQL_CONNECTION',
      useFactory: async () => {
        return mysql.createPool({
          host: '88.214.206.127',
          user: 'admin',
          password: 'M1r7wzyVVBiclLEj7Vj+VG+1JXxs/xtcdCmmPRyw3qg=',
          database: 'WLS',
          waitForConnections: true,
          connectionLimit: 50,
          queueLimit: 0,
        });
      },
    },
  ],
  exports: ['MYSQL_CONNECTION'],
})
export class MysqlModule {}
