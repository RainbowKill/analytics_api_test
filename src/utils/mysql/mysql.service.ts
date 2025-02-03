import { Injectable, Inject } from '@nestjs/common';
import { Pool } from 'mysql2/promise';

@Injectable()
export class MySqlService {
  constructor(
    @Inject('MYSQL_CONNECTION') private readonly connection: Pool,
  ) {}

  async query<T>(query: string, params: any[] = []): Promise<T[]> {
    const [rows] = await this.connection.query(query, params);
    return rows as T[];
  }

  async execute(query: string, params: any[] = []): Promise<void> {
    await this.connection.execute(query, params);
  }

  async findAll(tableName: string): Promise<any[]> {
    return this.query(`SELECT * FROM ${tableName}`);
  }

  async findById(tableName: string, id: number): Promise<any> {
    const rows = await this.query(`SELECT * FROM ${tableName} WHERE id = ?`, [id]);
    return rows[0];
  }

  async insert(tableName: string, data: Record<string, any>): Promise<void> {
    const keys = Object.keys(data).join(', ');
    const values = Object.values(data);
    const placeholders = values.map(() => '?').join(', ');

    await this.execute(
      `INSERT INTO ${tableName} (${keys}) VALUES (${placeholders})`,
      values,
    );
  }

  async updateById(
    tableName: string,
    id: number,
    data: Record<string, any>,
  ): Promise<void> {
    const updates = Object.keys(data)
      .map((key) => `${key} = ?`)
      .join(', ');
    const values = Object.values(data);

    await this.execute(
      `UPDATE ${tableName} SET ${updates} WHERE id = ?`,
      [...values, id],
    );
  }

  async deleteById(tableName: string, id: number): Promise<void> {
    await this.execute(`DELETE FROM ${tableName} WHERE id = ?`, [id]);
  }
}
