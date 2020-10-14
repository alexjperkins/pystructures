"""
Problem Statement:

Construct a QuadTree DataStructure

### --- QuadTree --- ###

If for example there is a box of particles, and the user would like to know
how far away a given particle is to other particles.
This is in it's basic form would result in a Time Complexity of n^2,
where n is the number of particles.

## Quadtree

A quadtree operates by splitting a given space into 4 sections recursivley,
whereby each `quad` child maintains a reference to the parent.

An important parameterization of Quadtree is the concept of capacity:
The capacity at which if a n=capacity points are within a  given rectangle of
a QuadTree, the rectangle undergoes subdivison.
"""


import itertools


class Point:
    __slots__ = ("x", "y")

    def __init__(self, *,  x: int, y: int):
        self.x = x
        self.y = y

    def __repr__(self):
        return f'Point: ({self.x}, {self.y})'

    def __iter__(self):
        yield from (self.x, self.y)


class Boundary:
    __slots__ = ("x", "y", "r")

    def __init__(self, *, x: int, y: int, r: int):
        self.y = y
        self.x = x
        self.r = r  # width

    def __repr__(self):
        return f'Boundary: ({self.x}, {self.y}, {self.r})'

    def __contains__(self, point: Point) -> bool:
        return all(
            (
                point.x <= self.x,
                point.x >= self.x - self.r,
                point.y <= self.y,
                point.y >= self.y - self.r,
            )
        )

    def split(self):
        x, y, r = self.x, self.y, self.r / 2
        pertubations = itertools.product((0, r), repeat=2)

        top_right_most_edges = (
            (x - dx, y - dy)
            for dx, dy in pertubations
        )

        yield from (
            self.__class__(x=x, y=y, r=r)
            for x, y in top_right_most_edges
        )


class QuadTree:
    def __init__(self, *, boundary: Boundary, max_capacity=4):
        self.boundary = boundary
        self._max_capacity = max_capacity
        self.points = []
        self.children = []

    def __len__(self):
        # TODO: Refactor
        return len(self.points) if self.points is not None else self._max_capacity + 1

    def insert(self, *, point: Point) -> Point:
        if len(self) >= self._max_capacity:
            if not self.children:
                self._subdivide()

        return self._insert(point)

    def pprint(self):
        if self.points:
            print('\n', self.boundary, '\n')
            for point in self.points:
                print(point, end=', ')

        for child in self.children:
            print(child)
            child.pprint()

    def _insert(self, point: Point) -> Point:
        if not self.children:
            self.points.append(point)
            return point

        for qt in self.children:
            if point in qt.boundary:
                qt.insert(point=point)
                break

        return point

    def _subdivide(self):
        print("Subdiving...")
        for boundary in self.boundary.split():
            self.children.append(
                self.__class__(
                    boundary=boundary,
                    max_capacity=self._max_capacity,
                )
            )

        while self.points:
            self.insert(point=self.points.pop())

        self.points = None
        return self.children

    def __repr__(self):
        return (
            'QuadTree: points: {p!r}, children: {c!r}'
            .format(p=self.points, c=self.children)
        )
