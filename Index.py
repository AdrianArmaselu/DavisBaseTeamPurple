import bisect
import os
import pickle


page_size = 512

class _NodeInTree(object):
    __buckets__ = ["tree", "value", "children"]

    def __init__(self, tree, value=None, children=None):
        self.tree = tree
        self.value = value or []
      
        self.children = children or []
        if self.children:
            assert len(self.value) + 1 == len(self.children)

    def __repr__(self):
        name = getattr(self, "children", 0) and "Branch" or "Leaf"
        return "<%s %s>" % (name, ", ".join(map(str, self.value)))

    def lateral(self, parent, parent_ind, dest, dest_ind):
        if parent_ind > dest_ind:
            dest.value.append(parent.value[dest_ind])
            if self.children:
                dest.children.append(self.children.pop(0))
        else:
            dest.value.insert(0, parent.value[parent_ind])
            parent.value[parent_ind] = self.value.pop()
            if self.children:
                dest.children.insert(0, self.children.pop())

    def contract(self, predecessor):
        parent = None
        if predecessor:
            parent, parent_ind = predecessor.pop()
            # try to lend to the left neighboring sibling
            if parent_ind:
                left_sib = parent.children[parent_ind - 1]
                if len(left_sib.value) < self.tree.order:
                    self.lateral(
                            parent, parent_ind, left_sib, parent_ind - 1)
                    return

            # try the right neighbor
            if parent_ind + 1 < len(parent.children):
                right_sib = parent.children[parent_ind + 1]
                if len(right_sib.value) < self.tree.order:
                    self.lateral(
                            parent, parent_ind, right_sib, parent_ind + 1)
                    return

        middle = len(self.value) // 2
        sibling, push = self.split()

        if not parent:
            parent, parent_ind = self.tree.BRANCH(
                    self.tree, children=[self]), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.value.insert(parent_ind, push)
        parent.children.insert(parent_ind + 1, sibling)
        if len(parent.value) > parent.tree.order:
            parent.contract(predecessor)

    def expand(self, predecessor):
        parent, parent_ind = predecessor.pop()
        minm = self.tree.order // 2
        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_ind + 1 < len(parent.children):
            right_sib = parent.children[parent_ind + 1]
            if len(right_sib.value) > minm:
                right_sib.lateral(parent, parent_ind + 1, self, parent_ind)
                return

        # try to borrow from the left sibling
        if parent_ind:
            left_sib = parent.children[parent_ind - 1]
            if len(left_sib.value) > minm:
                left_sib.lateral(parent, parent_ind - 1, self, parent_ind)
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.value.append(parent.value[parent_ind - 1])
            left_sib.value.extend(self.value)
            if self.children:
                left_sib.children.extend(self.children)
            parent.value.pop(parent_ind - 1)
            parent.children.pop(parent_ind)
        else:
            self.value.append(parent.value[parent_ind])
            self.value.extend(right_sib.value)
            if self.children:
                self.children.extend(right_sib.children)
            parent.value.pop(parent_ind)
            parent.children.pop(parent_ind + 1)

        if len(parent.value) < minm:
            if predecessor:
                # parent is not the root
                parent.expand(predecessor)
            elif not parent.value:
                # parent is root, and its now empty
                self.tree._root = left_sib or self

    def split(self):
        middle = len(self.value) // 2
        median = self.value[middle]
        sibling = type(self)(
                self.tree,
                self.value[middle + 1:],
                self.children[middle + 1:])
        self.value = self.value[:middle]
        self.children = self.children[:middle + 1]
        return sibling, median

    def insert(self, ind, element, predecessor):
        self.value.insert(ind, element)
        if len(self.value) > self.tree.order:
            self.contract(predecessor)

    def remove(self, ind, predecessor):
        minm = self.tree.order // 2

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            add_ancestors = [(self, ind + 1)]
            descendent = self.children[ind + 1]
            while descendent.children:
                add_ancestors.append((descendent, 0))
                descendent = descendent.children[0]
            if len(descendent.value) > minm:
                predecessor.extend(add_ancestors)
                self.value[ind] = descendent.value[0]
                descendent.remove(0, predecessor)
                return

            # fall back to the left child
            add_ancestors = [(self, ind)]
            descendent = self.children[ind]
            while descendent.children:
                add_ancestors.append(
                        (descendent, len(descendent.children) - 1))
                descendent = descendent.children[-1]
            predecessor.extend(add_ancestors)
            self.value[ind] = descendent.value[-1]
            descendent.remove(len(descendent.children) - 1, predecessor)
        else:
            self.value.pop(ind)
            if len(self.value) < minm and predecessor:
                self.expand(predecessor)




class Index_Btree(object):
    BRANCH = LEAF = _NodeInTree

    def __init__(self, order):
        self.order = order
        self._root = self._bottom = self.LEAF(self)

    def _path_to(self, element):
        curr = self._root
        ancestry = []

        while getattr(curr, "children", None):
            ind = bisect.bisect_left(curr.value, element)
            ancestry.append((curr, ind))
            if ind < len(curr.value) \
                    and curr.value[ind] == element:
                return ancestry
            curr = curr.children[ind]

        ind = bisect.bisect_left(curr.value, element)
        ancestry.append((curr, ind))
        present = ind < len(curr.value)
        present = present and curr.value[ind] == element

        return ancestry

    def _current(self, element, predecessor):
        last, ind = predecessor[-1]
        return ind < len(last.value) and last.value[ind] == element

    def insert(self, element, ):
        curr = self._root
        predecessor = self._path_to(element)
        node, ind = predecessor[-1]
        while getattr(node, "children", None):
            node = node.children[ind]
            ind = bisect.bisect_left(node.value, element)
            predecessor.append((node, ind))
        node, ind = predecessor.pop()
        node.insert(ind, element, predecessor)

    def search(self, element):
        curr = self._root
        if element in dict(self):
            return dict(self)[element]
        return None

    def remove(self, element):
        if self.search(element):
            element = [element, (self.search(element))]
        else:
            element = [element, None]
        curr = self._root
        predecessor = self._path_to(element)
        if self._current(element, predecessor):
            node, ind = predecessor.pop()
            node.remove(ind, predecessor)
        else:
            raise ValueError("%r not in %s" % (element, self.__class__.__name__))

    

    def __contains__(self, element):
        return self._current(element, self._path_to(element))

    def __iter__(self):
        def _recurse(node):
            if node.children:
                for child, element in zip(node.children, node.value):
                    for child_item in _recurse(child):
                        yield child_item
                    yield element
                for child_item in _recurse(node.children[-1]):
                    yield child_item
            else:
                for element in node.value:
                    yield element

        for element in _recurse(self._root):
            yield element

    

    def __repr__(self):
        def recurse(node, accum, depth):
            accum.append(("  " * depth) + repr(node))
            for node in getattr(node, "children", []):
                recurse(node, accum, depth + 1)

        accum = []
        recurse(self._root, accum, 0)
        return "\n".join(accum)


def insert_index_entry(table_name, key):
    file_list = os.listdir(data_dir)
    table_file = str(table_name) + ".ndx"

    if table_file not in file_list:
        file = os.path.join(data_dir, table_file)
    else:
        file = create_table(table_file)
    return 

def write_to_file(record):
    file_list = os.listdir(self.index_dir)
    filename = str(self.table_name) + str(self.column_name) + ".ndx"
    return

def initialize_tree(table_name, column_name, new_tree):
    filename = str(table_name) + "_" + str(column_name) + ".ndx"
    write_tree_to_file(filename, new_tree)
    return

def write_tree_to_file(filename, new_tree):
    with open(filename, "wb") as f:
        pickle.dump(new_tree, f)
    return


def read_tree_from_file(table_name, column_name):
    filename = str(table_name) + "_" + str(column_name) + ".ndx"
    with open(filename, "rb") as f:
        tree = pickle.load(f)
    return tree

b = Index_Btree(5)
b.insert(['2', '5'])
b.insert(['7', '5'])
b.insert(['9', '5'])
b.insert(['3', '4'])
b.insert(['5', '4'])
b.insert(['1', '4'])
b.insert(['18', '4'])
b.insert(['11', '5'])
b.insert(['12', '5'])
b.insert(['13', '5'])
b.insert(['14', '4'])
b.insert(['15', '5'])
b.insert(['16', '5'])
b.insert(['17', '5'])
b.insert(['19', '4'])
print(b)
b.remove('17')
print(b)
print('\n\n')
value = b.search('11')
print(value)
print('\n\n\n')
initialize_tree('test', 'test', b)
tree = read_tree_from_file('test', 'test')
tree.insert(['22','22'])
print(tree)
