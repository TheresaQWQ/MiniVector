import { Vector } from "./vector";
import { getClient } from "./engine";
import { Knex } from "knex";

export interface Config {
  vector: {
    max: number
    min: number
    size: number
  }
  db: {
    table: string
  },
  cluster: {
    maxDistance: number
    partition: number
  }
}

export interface QueryResult {
  data: string
  distance: number
  vector: Vector
}

export class Store {
  private readonly config: Config

  private table_index: string = ''
  private table_vector: string = ''

  private ClusterMaxDistance: number = 0

  private ClusterCache: Map<number, Vector> = new Map()

  private dbClients: Map<number, Knex> = new Map()

  constructor(config: Config) {
    this.config = config

    this.table_index = `${this.config.db.table}_index`
    this.table_vector = `${this.config.db.table}_vector`

    this.ClusterMaxDistance = this.config.cluster.maxDistance
  }

  private async db(index: number, metadata?: boolean): Promise<Knex> {
    const client = (this.dbClients.has(index) ? this.dbClients.get(index) : getClient(this.config.db.table, index.toString())) as Knex

    if (!this.dbClients.has(index)) {
      if (metadata) {
        await client.schema.createTableIfNotExists(this.table_index, (table) => {
          table.increments('cluster_id').primary() // 集群ID
          table.string('vector').notNullable() // 集群中心向量
        })
      }
  
      await client.schema.createTableIfNotExists(this.table_vector, (table) => {
        table.increments('id').primary() // 数据ID
        table.integer('cluster_id').notNullable() // 所属集群ID
        table.string('vector').notNullable() // 数据向量
        table.string('data').notNullable() // 数据内容
      })

      this.dbClients.set(index, client)
  
      return client
    }

    return client
  }

  private async getClientIndex(cluster_id: number) {
    return cluster_id % this.config.cluster.partition
  }

  private encodeVector(vector: Vector) {
    return JSON.stringify(vector.__vector)
  }

  private decodeVector(vector: string) {
    return new Vector(JSON.parse(vector), this.config.vector.max, this.config.vector.min)
  }

  public arr2vec(arr: number[]): Vector {
    if (arr.length !== this.config.vector.size) {
      throw new Error('Vector size mismatch')
    }

    return new Vector(arr, this.config.vector.max, this.config.vector.min)
  }

  private async getClusterList(key?: number) {
    if (this.ClusterCache.size === 0) {
      await this.PostChanges_ClusterCache()
    }

    if (key) {
      return {
        cluster_id: key,
        vector: this.ClusterCache.get(key)
      }
    } else {
      const list: {
        cluster_id: number
        vector: Vector
      }[] = []

      for (const [key, value] of this.ClusterCache) {
        list.push({
          cluster_id: key,
          vector: value
        })
      }

      return list
    }
  }

  private async PostChanges_ClusterCache(mode: 'refresh' | 'update' = 'refresh', key?: number, value?: Vector) {
    if (mode === 'refresh') {
      this.ClusterCache.clear()
      const client = await this.db(0, true)
      const clusters = await client(this.table_index).select('*')
      for (const cluster of clusters) {
        this.ClusterCache.set(cluster.cluster_id, this.decodeVector(cluster.vector))
      }
    } else if (mode === 'update' && key && value) {
      this.ClusterCache.set(key, value)
    }
  }

  private async getCenterVector(clusterID: number) {
    const client = await this.db(await this.getClientIndex(clusterID))
    const vectors = await client(this.table_vector).select('vector').where('cluster_id', clusterID)
    const centerVector = new Vector(Array(this.config.vector.size).fill(0), this.config.vector.max, this.config.vector.min)

    for (const vector of vectors) {
      const v = await this.decodeVector(vector.vector)
      for (let i = 0; i < this.config.vector.size; i++) {
        centerVector.__vector[i] += v.__vector[i]
      }
    }

    for (let i = 0; i < this.config.vector.size; i++) {
      centerVector.__vector[i] /= vectors.length
    }

    return centerVector
  }

  private async getClusterID(vector: Vector) {
    const clusters = await this.getClusterList() as { cluster_id: number, vector: Vector }[]
    let minDistance = Number.MAX_VALUE
    let clusterID = -1

    for (let i = 0; i < clusters.length; i++) {
      const cluster = clusters[i]
      const distance = vector.distance(cluster.vector)

      if (distance < minDistance && distance < this.ClusterMaxDistance) {
        minDistance = distance
        clusterID = cluster.cluster_id
      }
    }

    if (clusterID === -1) {
      clusterID = clusters.length
      const client = await this.db(0)
      await client(this.table_index).insert({
        cluster_id: clusterID,
        vector: await this.encodeVector(vector)
      })

      await this.PostChanges_ClusterCache('update', clusterID, vector)
    }

    return clusterID
  }

  private async updateClusterCenter(clusterID: number) {
    const centerVector = await this.getCenterVector(clusterID)
    const IndexDB = await this.db(0)
    await IndexDB(this.table_index).where('cluster_id', clusterID).update({ vector: await this.encodeVector(centerVector) })
    await this.PostChanges_ClusterCache('update', clusterID, centerVector)

    const client = await this.db(await this.getClientIndex(clusterID))
    const vectors = await client(this.table_vector).select('id', 'vector').where('cluster_id', clusterID)

    for (let i = 0; i < vectors.length; i++) {
      const vector = vectors[i]
      const v = await this.decodeVector(vector.vector)
      const distance = centerVector.distance(v)

      if (distance > this.ClusterMaxDistance) {
        this.getClusterID(v).then(async (newClusterID) => {
          const beforeIndex = await this.getClientIndex(clusterID)
          const afterIndex = await this.getClientIndex(newClusterID)

          const beforeClient = await this.db(beforeIndex)
          const afterClient = await this.db(afterIndex)

          await beforeClient(this.table_vector).where('id', vector.id).del()
          await afterClient(this.table_vector).insert({
            cluster_id: newClusterID,
            vector: await this.encodeVector(v),
            data: await beforeClient(this.table_vector).select('data').where('id', vector.id).first()
          })

          await this.updateClusterCenter(newClusterID)
        })
      }
    }
  }

  public async insert(data: string, vector: Vector) {
    const clusterID = await this.getClusterID(vector)
    const client = await this.db(await this.getClientIndex(clusterID))

    await client(this.table_vector).insert({
      cluster_id: clusterID,
      vector: await this.encodeVector(vector),
      data
    })

    this.updateClusterCenter(clusterID)

    return clusterID
  }

  public async query(vector: Vector, limit: number = 10): Promise<QueryResult[]> {
    const clusterID = await this.getClusterID(vector)
    const client = await this.db(await this.getClientIndex(clusterID))
    const vectors = await client(this.table_vector).select('*').where('cluster_id', clusterID)

    const result: QueryResult[] = []

    for (const item of vectors) {
      const v = await this.decodeVector(item.vector)
      result.push({
        data: item.data,
        distance: vector.distance(v),
        vector: v
      })
    }

    return result.sort((a, b) => a.distance - b.distance).slice(0, limit)
  }
}