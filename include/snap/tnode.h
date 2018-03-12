#ifndef _TNODE_H
#define _TNODE_H

#include <snap/readers.h>

class TNode {
    private:
        //key (5 bytes), nelements (5bytes) strat (1 byte), file (2 bytes), pos (5 bytes)
        const char *rawblock;

    public:
        TNode() : rawblock(NULL) {}

        TNode(const TNode *node) {
            this->rawblock = node->rawblock;
        }

        TNode& operator++ (int) {
            rawblock += 31;
            return *this;
        }

        bool operator < (const TNode& Node) const {
            return rawblock < Node.rawblock;
        }

        bool operator == (const TNode& Node) const {
            return Node.rawblock == rawblock;
        }

        TNode(const char *rawblock) : rawblock(rawblock) {
        }

        /// Returns ID of the current node.
        int64_t GetId() const {
            const int64_t v = (*(int64_t*)(rawblock)) & 0XFFFFFFFFFFl;
            //return Utils::decode_longFixedBytes(rawblock, 5);
            return v;
        }

        /// Returns degree of the current node, the sum of in-degree and out-degree.
        int64_t GetDeg() const {
            return GetInDeg() + GetOutDeg();
        }

        /// Returns in-degree of the current node.
        int64_t GetInDeg() const {
            const int64_t v = (*(int64_t*)(rawblock + 18)) & 0XFFFFFFFFFFl;
            return v;
        }

        /// Returns out-degree of the current node.
        int64_t GetOutDeg() const {
            const int64_t v = (*(int64_t*)(rawblock + 5)) & 0XFFFFFFFFFFl;
            return v;
        }

        /// Returns ID of NodeN-th in-node (the node pointing to the current node). ##TNGraph::TNodeI::GetInNId
        int64_t GetInNId(const int64_t& NodeN) const {
            //This code does not work for newcolumn reader because there I would need to further advance to remove some initial bytes that store metadata. However, newcolumn layout is never triggered so it should be fine
            const uint8_t strat = rawblock[23];
            SnapReaders::pReader reader = SnapReaders::readers[strat];
            const short file = *(short*)(rawblock + 24);
            const char *p = SnapReaders::f_osp[file];
            const int64_t pos = (*(int64_t*)(rawblock + 26)) & 0XFFFFFFFFFFl;
            const char *osp = p + pos;
            return reader(osp, NodeN);
        }

        /// Returns ID of NodeN-th out-node (the node the current node points to). ##TNGraph::TNodeI::GetOutNId
        int64_t GetOutNId(const int64_t& NodeN) const {
            //The same warning as before applies also to here
            const uint8_t strat = rawblock[10];
            SnapReaders::pReader reader = SnapReaders::readers[strat];
            const short file = *(short*)(rawblock + 11);
            const char *p = SnapReaders::f_sop[file];
            const int64_t pos = (*(int64_t*)(rawblock + 13)) & 0XFFFFFFFFFFl;
            const char *sop = p + pos;
            return reader(sop, NodeN);
        }

        SnapReaders::pReader GetOutReader() const {
            const uint8_t strat = rawblock[10];
            return SnapReaders::readers[strat];
        }

        SnapReaders::pReader GetInReader() const {
            const uint8_t strat = rawblock[23];
            return SnapReaders::readers[strat];
        }

        int GetOutLenField() const {
            const uint8_t strat = rawblock[10];
            //const int storageType = StorageStrat::getStorageType(strat);
            //assert(storageType == NEWROW_ITR);
            const char nbytes1 = (strat >> 3) & 3;
            switch (nbytes1) {
                case 0:
                    return 1;
                case 1:
                    return 2;
                case 2:
                    return 4;
                case 3:
                    return 5;
                default:
                    throw 10;
            }
        }

        int GetInLenField() const {
            const uint8_t strat = rawblock[23];
            const char nbytes1 = (strat >> 3) & 3;
            switch (nbytes1) {
                case 0:
                    return 1;
                case 1:
                    return 2;
                case 2:
                    return 4;
                case 3:
                    return 5;
                default:
                    throw 10;
            }
        }

        const char *GetBeginOut() const {
            const short file = *(short*)(rawblock + 11);
            const char *p = SnapReaders::f_sop[file];
            const int64_t pos = (*(int64_t*)(rawblock + 13)) & 0XFFFFFFFFFFl;
            const char *sop = p + pos;
            return sop;
        }

        const char *GetBeginIn() const {
            const short file = *(short*)(rawblock + 24);
            const char *p = SnapReaders::f_osp[file];
            const int64_t pos = (*(int64_t*)(rawblock + 26)) & 0XFFFFFFFFFFl;
            const char *osp = p + pos;
            return osp;
        }

        /// Returns ID of NodeN-th neighboring node. ##TNGraph::TNodeI::GetNbrNId
        int64_t GetNbrNId(const int64_t& NodeN) const {
            if (NodeN < GetInDeg()) {
                return GetInNId(NodeN);
            } else {
                return GetOutNId(NodeN - GetInDeg());
            }
        }

        /// Tests whether node with ID NId points to the current node.
        bool IsInNId(const int64_t& NId) const {
            throw 10; //not implemented
        }

        /// Tests whether the current node points to node with ID NId.
        bool IsOutNId(const int64_t& NId) const {
            throw 10; //not defined for directed graph
        }

        /// Tests whether node with ID NId is a neighbor of the current node.
        bool IsNbrNId(const int64_t& NId) const {
            throw 10; //not defined for directed graph
        }
};


#endif
