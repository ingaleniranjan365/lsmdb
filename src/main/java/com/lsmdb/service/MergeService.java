package com.lsmdb.service;

import com.lsmdb.entity.SegmentIndex;
import com.lsmdb.entity.Metadata;
import com.lsmdb.entity.merge.HeapElement;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import static com.lsmdb.entity.merge.HeapElement.getHeapElementComparator;
import static com.lsmdb.entity.merge.HeapElement.isIdPresentInList;

public class MergeService {

        private final FileIOService fileIOService;

        public MergeService(FileIOService fileIOService) {
                this.fileIOService = fileIOService;
        }

        public Map<String, Metadata> merge(
                List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
                final String mergeSegmentPath
        )
                throws IOException {
                var mergeSegment = new File(mergeSegmentPath);
                final Map<String, Metadata> mergedSegmentIndex = new LinkedHashMap<>();
                var heap = new PriorityQueue<>(getHeapElementComparator());

                heap.addAll(getSeedElements(segmentIndexEnumeration));

                var last = new HeapElement("", -1);
                while (!heap.isEmpty()) {
                        var candidate = findNextCandidate(segmentIndexEnumeration, heap, last);
                        if (couldNotFindNextElementToProcess(heap, last, candidate)) {
                                break;
                        }
                        var segmentIndex = segmentIndexEnumeration.get(candidate.getIndex()).right;
                        var metadata = segmentIndex.getIndex().get(candidate.getId());
                        var in = fileIOService.readBytes(
                                segmentIndex.getSegment().getSegmentPath(),
                                metadata
                        );
                        mergedSegmentIndex.put(candidate.getId(),
                                new Metadata(mergeSegment.length(), in.length, metadata.getInstant()));
                        FileUtils.writeByteArrayToFile(mergeSegment, in, true);

                        addNextHeapElementForSegment(segmentIndexEnumeration, heap, candidate);
                        last = candidate;
                }
                return mergedSegmentIndex;
        }

        private HeapElement findNextCandidate(
                List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
                PriorityQueue<HeapElement> heap, HeapElement last) {
                var next = heap.remove();
                while (next.getId().equals(last.getId())) {
                        addNextHeapElementForSegment(segmentIndexEnumeration, heap, next);
                        if (heap.isEmpty()) {
                                break;
                        }
                        next = heap.remove();
                }
                return next;
        }

        private boolean couldNotFindNextElementToProcess(PriorityQueue<HeapElement> heap, HeapElement last,
                                                         HeapElement next) {
                return heap.isEmpty() && next.getId().equals(last.getId());
        }

        private void addNextHeapElementForSegment(
                List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration,
                PriorityQueue<HeapElement> heap, HeapElement next) {
                var iterator = segmentIndexEnumeration.get(next.getIndex()).left;
                if (iterator.hasMoreElements()) {
                        heap.add(new HeapElement(iterator.nextElement(), next.getIndex()));
                }
        }

        private LinkedList<HeapElement> getSeedElements(
                List<ImmutablePair<Enumeration<String>, SegmentIndex>> segmentIndexEnumeration) {
                var firstElements = new LinkedList<HeapElement>();
                int itr = 0;
                while (itr < segmentIndexEnumeration.size()) {
                        var next = new HeapElement(segmentIndexEnumeration.get(itr).left.nextElement(), itr);
                        while (isIdPresentInList(firstElements, next.getId())) {
                                if (segmentIndexEnumeration.get(itr).left.hasMoreElements()) {
                                        next = new HeapElement(segmentIndexEnumeration.get(itr).left.nextElement(),
                                                itr);
                                } else {
                                        break;
                                }
                        }
                        if (!isIdPresentInList(firstElements, next.getId())) {
                                firstElements.add(next);
                        }
                        itr += 1;
                }
                return firstElements;
        }

}
