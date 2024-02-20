import { Store } from './lib/store'

const store = new Store({
  vector: {
    max: 1,
    min: -1,
    size: 100
  },
  db: {
    table: 'test'
  },
  cluster: {
    maxDistance: 0.4,
    partition: 10
  }
});

(async () => {
  for (let i = 0; i < 50000; i++) {
    const vector = Array(100).fill(0).map(() => Math.random() * 2 - 1)
    const v = store.arr2vec(vector)

    console.time(`data_${i}`)
    // await store.insert(`data_${i}`, v)

    const result = await store.query(v, 10)
    console.timeEnd(`data_${i}`)

    // result.length > 0 && console.log(result)
  }

  console.log('done')
})()

// TODO: 完成数据删除功能和数据更新功能