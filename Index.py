import bisect
import itertools
import operator
import os


page_size = 512


### INITIAL COMMIT IS NOT THE CORRECT VERSION

# def write_to_file(tree, table_name):
#     file = os.path.join(table_name + '.ndx')
#     f = open(file, "w")
#     f.write(str(tree))
#     f.close()
#     return

# def read_file(table_name):
#     file = os.path.join(table_name + '.ndx')
#     f = open(file, "r")
#     data = f.read()
#     f.close()
#     print(data)
#     return data

class Index(Page):
    def create_index():

class _BNode(object):
    __slots__ = ["tree", "value", "children"]

    def __init__(self, tree, value=None, children=None):
        self.tree = tree
        self.value = value or []
      
        self.children = children or []
        if self.children:
            assert len(self.value) + 1 == len(self.children),
                    "one more child than data item required"

    def __repr__(self):
        name = getattr(self, "children", 0) and "Branch" or "Leaf"
        return "<%s %s>" % (name, ", ".join(map(str, self.value)))

    def lateral(self, parent, parent_ind, dest, dest_ind):
        print('lateral')
        if parent_ind > dest_ind:
            dest.value.append(parent.value[dest_ind])
            parent.value[dest_ind] = self.value.pop(0)
            if self.children:
                dest.children.append(self.children.pop(0))
        else:
            dest.value.insert(0, parent.value[parent_ind])
            parent.value[parent_ind] = self.value.pop()
            if self.children:
                dest.children.insert(0, self.children.pop())

    def shrink(self, ancestors):
        parent = None
        print('shrink')
        if ancestors:
            parent, parent_ind = ancestors.pop()
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
            parent.shrink(ancestors)

    def grow(self, ancestors):
        parent, parent_ind = ancestors.pop()
        print('grow')
        minimum = self.tree.order // 2
        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_ind + 1 < len(parent.children):
            right_sib = parent.children[parent_ind + 1]
            if len(right_sib.value) > minimum:
                right_sib.lateral(parent, parent_ind + 1, self, parent_ind)
                return

        # try to borrow from the left sibling
        if parent_ind:
            left_sib = parent.children[parent_ind - 1]
            if len(left_sib.value) > minimum:
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

        if len(parent.value) < minimum:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
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

    def insert(self, ind, item, ancestors):
        self.value.insert(ind, item)
        if len(self.value) > self.tree.order:
            self.shrink(ancestors)

    def remove(self, ind, ancestors):
        minimum = self.tree.order // 2

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, ind + 1)]
            descendent = self.children[ind + 1]
            while descendent.children:
                additional_ancestors.append((descendent, 0))
                descendent = descendent.children[0]
            if len(descendent.value) > minimum:
                ancestors.extend(additional_ancestors)
                self.value[ind] = descendent.value[0]
                descendent.remove(0, ancestors)
                return

            # fall back to the left child
            additional_ancestors = [(self, ind)]
            descendent = self.children[ind]
            while descendent.children:
                additional_ancestors.append(
                        (descendent, len(descendent.children) - 1))
                descendent = descendent.children[-1]
            ancestors.extend(additional_ancestors)
            self.value[ind] = descendent.value[-1]
            descendent.remove(len(descendent.children) - 1, ancestors)
        else:
            self.value.pop(ind)
            if len(self.value) < minimum and ancestors:
                self.grow(ancestors)




class BTree(object):
    BRANCH = LEAF = _BNode

    def __init__(self, order):
        self.order = order
        self._root = self._bottom = self.LEAF(self)

    def _path_to(self, item):
        current = self._root
        ancestry = []

        while getattr(current, "children", None):
            ind = bisect.bisect_left(current.value, item)
            ancestry.append((current, ind))
            if ind < len(current.value) \
                    and current.value[ind] == item:
                return ancestry
            current = current.children[ind]

        ind = bisect.bisect_left(current.value, item)
        ancestry.append((current, ind))
        present = ind < len(current.value)
        present = present and current.value[ind] == item

        return ancestry

    def _current(self, item, ancestors):
        last, ind = ancestors[-1]
        return ind < len(last.value) and last.value[ind] == item

    def insert(self, item):
        current = self._root
        ancestors = self._path_to(item)
        node, ind = ancestors[-1]
        while getattr(node, "children", None):
            node = node.children[ind]
            ind = bisect.bisect_left(node.value, item)
            ancestors.append((node, ind))
        node, ind = ancestors.pop()
        node.insert(ind, item, ancestors)

    def remove(self, item):
        current = self._root
        ancestors = self._path_to(item)

        if self._current(item, ancestors):
            node, ind = ancestors.pop()
            node.remove(ind, ancestors)
        else:
            raise ValueError("%r not in %s" % (item, self.__class__.__name__))

    def __contains__(self, item):
        return self._current(item, self._path_to(item))

    def __iter__(self):
        def _recurse(node):
            if node.children:
                for child, item in zip(node.children, node.value):
                    for child_item in _recurse(child):
                        yield child_item
                    yield item
                for child_item in _recurse(node.children[-1]):
                    yield child_item
            else:
                for item in node.value:
                    yield item

        for item in _recurse(self._root):
            yield item

    

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


b = BTree(10)
#b.insert('2', '3')


print(b) 

