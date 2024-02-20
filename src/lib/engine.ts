import knex from 'knex'
import path from 'path'
import fs from 'fs';

export const getClient = (db: string, filename: string) => {
  try {
    fs.mkdirSync(path.join(process.cwd(), `./data/${db}`), { recursive: true })
  } catch (error) {}

  return knex({
    client: 'better-sqlite3',
    connection: {
      filename: path.join(process.cwd(), `./data/${db}/${filename}.db`)
    }
  })
}