package join.algorithms;

import java.util.function.Consumer;

import join.datastructures.Block;
import join.datastructures.Relation;
import join.datastructures.Tuple;
import join.manager.BlockManager;

public class HashEquiJoin implements Join {
	protected final int numBuckets;
	protected final BlockManager blockManager;

	public HashEquiJoin(int numBuckets, BlockManager blockManager) {
		this.numBuckets = numBuckets;
		this.blockManager = blockManager;
	}

	@Override
	public void join(Relation relation1, int joinAttribute1, Relation relation2, int joinAttribute2,
			Consumer<Tuple> consumer) {

		Relation[] relation1HashTable= setupHashTable(relation1, joinAttribute1);
		Relation[] relation2HashTable = setupHashTable(relation2, joinAttribute2);

		NestedLoopEquiJoin nestedLoopJoin = new NestedLoopEquiJoin(blockManager);
		for (int i = 0; i < numBuckets; ++i) {
			// TODO: join
			// pro hashwert: nutzen von nestedloopjoin
			nestedLoopJoin.join(relation1HashTable[i], joinAttribute1, relation2HashTable[i], joinAttribute2, consumer);
		}
	}

	@Override
	public int getIOEstimate(Relation relation1, Relation relation2) {
		return 3 * (relation1.getBlockCount() + relation2.getBlockCount());
	}

	private int getHashValue(Tuple tuple, int joinAttribute){
		return Math.abs(tuple.getData(joinAttribute).hashCode()) % this.numBuckets;
	}

	private Relation[] setupHashTable(Relation relation, int joinAttribute){
		// annahme: numbuckets < freie buckets
		// iteration relation 1
		// hash into numbuckts relations (max. 1 block pro relation gepinnt)
		// 2 datenstrukturen: hashwert-relation, haswert-buffered-bucket
		// alles auf disk
		Relation[] relationHashTable = new Relation[this.numBuckets];
		Block[] bufferedBlockHashTable = new Block[this.numBuckets];

		for(Block block: relation){
			blockManager.pin(block);
			for(Tuple tuple: block){
				int hashValue = getHashValue(tuple, joinAttribute);
				if(relationHashTable[hashValue] == null){
					relationHashTable[hashValue] = new Relation(false);
				}
				Relation bucketRelation = relationHashTable[hashValue];
				if(bufferedBlockHashTable[hashValue] == null){
					Block newBucketBlock = bucketRelation.getFreeBlock(blockManager);
					blockManager.pin(newBucketBlock);
					bufferedBlockHashTable[hashValue] = newBucketBlock;
				}
				Block bucketBlock = bufferedBlockHashTable[hashValue];
				if(!bucketBlock.addTuple(tuple)){
					// bucketBlock is full
					blockManager.unpin(bucketBlock);
					Block newBucketBlock = bucketRelation.getFreeBlock(blockManager);
					blockManager.pin(newBucketBlock);
					bufferedBlockHashTable[hashValue] = newBucketBlock;
					newBucketBlock.addTuple(tuple);
				}
			}
			blockManager.unpin(block);
		}
		// Flush
		for(Block block: bufferedBlockHashTable) {
			if (block == null)
				continue;
			blockManager.unpin(block);
		}

		return relationHashTable;
	}
}
