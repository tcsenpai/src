from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import deque

@dataclass
class Node:
    keys: List[Any]
    values: List[List[int]]  # List of row IDs for each key (handling duplicates)
    children: List['Node']
    is_leaf: bool = True

class BTreeIndex:
    def __init__(self, order: int = 100):
        self.root = Node([], [], [])
        self.order = order  # Maximum number of children per node
    
    def insert(self, key: Any, row_id: int) -> None:
        """Insert a key-value pair into the B-tree"""
        if len(self.root.keys) == (2 * self.order) - 1:
            # Split root if full
            new_root = Node([], [], [], False)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key, row_id)
    
    def search(self, key: Any) -> List[int]:
        """Search for a key and return all matching row IDs"""
        return self._search_node(self.root, key)
    
    def range_search(self, start_key: Any, end_key: Any) -> List[int]:
        """Perform a range search and return all matching row IDs"""
        result = []
        self._range_search_node(self.root, start_key, end_key, result)
        return result
    
    def _split_child(self, parent: Node, child_index: int) -> None:
        """Split a full child node"""
        order = self.order
        child = parent.children[child_index]
        new_node = Node([], [], [], child.is_leaf)
        
        # Move the median key to the parent
        median = order - 1
        parent.keys.insert(child_index, child.keys[median])
        parent.values.insert(child_index, child.values[median])
        parent.children.insert(child_index + 1, new_node)
        
        # Move half of the keys to the new node
        new_node.keys = child.keys[median + 1:]
        new_node.values = child.values[median + 1:]
        child.keys = child.keys[:median]
        child.values = child.values[:median]
        
        # Move children if not a leaf
        if not child.is_leaf:
            new_node.children = child.children[median + 1:]
            child.children = child.children[:median + 1]
    
    def _insert_non_full(self, node: Node, key: Any, row_id: int) -> None:
        """Insert into a non-full node"""
        i = len(node.keys) - 1
        
        if node.is_leaf:
            # Insert into leaf node
            while i >= 0 and self._compare_keys(key, node.keys[i]) < 0:
                i -= 1
            i += 1
            
            # Handle duplicate keys
            if i > 0 and self._compare_keys(key, node.keys[i-1]) == 0:
                node.values[i-1].append(row_id)
            else:
                node.keys.insert(i, key)
                node.values.insert(i, [row_id])
        else:
            # Find the child to insert into
            while i >= 0 and self._compare_keys(key, node.keys[i]) < 0:
                i -= 1
            i += 1
            
            if len(node.children[i].keys) == (2 * self.order) - 1:
                self._split_child(node, i)
                if self._compare_keys(key, node.keys[i]) > 0:
                    i += 1
            
            self._insert_non_full(node.children[i], key, row_id)
    
    def _search_node(self, node: Node, key: Any) -> List[int]:
        """Search for a key in a node"""
        i = 0
        while i < len(node.keys) and self._compare_keys(key, node.keys[i]) > 0:
            i += 1
        
        if i < len(node.keys) and self._compare_keys(key, node.keys[i]) == 0:
            return node.values[i]
        elif node.is_leaf:
            return []
        else:
            return self._search_node(node.children[i], key)
    
    def _range_search_node(self, node: Node, start_key: Any, end_key: Any, result: List[int]) -> None:
        """Perform range search on a node"""
        i = 0
        while i < len(node.keys) and self._compare_keys(start_key, node.keys[i]) > 0:
            i += 1
        
        if node.is_leaf:
            while i < len(node.keys) and self._compare_keys(node.keys[i], end_key) <= 0:
                result.extend(node.values[i])
                i += 1
        else:
            if i < len(node.keys):
                self._range_search_node(node.children[i], start_key, end_key, result)
            while i < len(node.keys) and self._compare_keys(node.keys[i], end_key) <= 0:
                result.extend(node.values[i])
                i += 1
                if i < len(node.children):
                    self._range_search_node(node.children[i], start_key, end_key, result)
    
    @staticmethod
    def _compare_keys(key1: Any, key2: Any) -> int:
        """Compare two keys, handling different types"""
        if key1 is None or key2 is None:
            if key1 is None and key2 is None:
                return 0
            return -1 if key1 is None else 1
        
        try:
            if key1 < key2:
                return -1
            elif key1 > key2:
                return 1
            return 0
        except TypeError:
            # Handle incomparable types
            return 0