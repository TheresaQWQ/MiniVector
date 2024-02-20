export class Vector {
  private readonly vector: number[] = []
  private readonly size: number = 0
  readonly max: number
  readonly min: number
  public ClusterID: number

  constructor(vector: number[], max: number, min: number) {
    this.vector = vector
    this.size = vector.length
    this.max = max
    this.min = min
    this.ClusterID = -1
  }
  
  public distance(targetVector: Vector) {
    const target = targetVector.__vector
    const targetSize = targetVector.__size

    if (this.size !== targetSize) {
      throw new Error('Vector size mismatch')
    }

    let dotProduct = 0
    let magnitudeA = 0
    let magnitudeB = 0

    for (let i = 0; i < this.size; i++) {
      dotProduct += this.vector[i] * target[i]
      magnitudeA += this.vector[i] ** 2
      magnitudeB += target[i] ** 2
    }

    magnitudeA = Math.sqrt(magnitudeA)
    magnitudeB = Math.sqrt(magnitudeB)

    const cosineSimilarity = dotProduct / (magnitudeA * magnitudeB)
    const normalizedDistance = (1 - cosineSimilarity) / 2

    return normalizedDistance
  }

  public equals(targetVector: Vector) {
    const target = targetVector.__vector
    const targetSize = targetVector.__size

    if (this.size !== targetSize) {
      throw new Error('Vector size mismatch')
    }

    for (let i = 0; i < this.size; i++) {
      if (this.vector[i] !== target[i]) {
        return false
      }
    }

    return true
  }

  public get __vector(): number[] {
    return this.vector
  }

  public get __size(): number {
    return this.size
  }

  public get config() {
    return {
      max: this.max,
      min: this.min
    }
  }
}