/**********************************************************/
/*              Programmed by Gerard Monells              */
/*               gerardmonells@hotmail.com                */
/**********************************************************/

#ifndef _AVLTREE_HPP
#define _AVLTREE_HPP
#include "wdbi.h"
//#include <iostream>
//using namespace std;
typedef unsigned int uint;
class AVLtree {
public:
  AVLtree(DataTable *dt);
  AVLtree(const AVLtree& a);
  AVLtree& operator=(const AVLtree& a);
  ~AVLtree();
  void add_item(int x);
  int *search(int x);
  int *search(void **ptr);
  uint size();
//  void preorder();
  void inorder(int *obf);
//  void postorder();

private:
  struct node {
    node() {};
    node(int x, node* l, node* r, uint h) {
      item = x;
      left = l;
      right = r;
      height = h;
    }
    int item;
    node* left;
    node* right;
    uint height;
  };
#ifdef AVL_TREE_FIX_MEM
node *nodearr;
int arrtop;
#endif
  DataTable *dt;
  node* root;
  uint num_nodes;
/*
  static AVLtree<T>::node* AVLtree<T>::copy_nodes(node *n);
  static void AVLtree<T>::destroy_nodes(node *n);
  static AVLtree<T>::node* AVLtree<T>::add_node(node* n, T x);
  static uint AVLtree<T>::max(uint a, uint b);
  static uint AVLtree<T>::height(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate_left(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate2_left(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate_right(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate2_right(node* n);
  static bool AVLtree<T>::search(node* n, T x);
  static void AVLtree<T>::preorder(node* n);
  static void AVLtree<T>::inorder(node* n);
  static void AVLtree<T>::postorder(node* n);
  */
  node* copy_nodes(node *n);
  node* add_node(node* n, int x);
  void destroy_nodes(node *n);
  //uint max(uint a, uint b);
  uint height(node* n);
  node* rotate_left(node* n);
  node* rotate2_left(node* n);
  node* rotate_right(node* n);
  node* rotate2_right(node* n);
  int * search(node* n, int x);
  int * search(node* n, void **ptr);
  //void preorder(node* n);
  void inorder(node* n,int *obf,int &off);
  //void postorder(node* n);
  int compare(int p1,int p2) {
	  return dt->CompareSortRow(p1,p2);
//	  if(p1>p2) return 1;
//	  if(p1==p2) return 0;
//	  return -1;
  };
  int compare(int p1,void **ptr) {
	  return dt->CompareSortedColumn(p1,ptr);
//	  if(p1>p2) return 1;
//	  if(p1==p2) return 0;
//	  return -1;
  };
#ifdef AVL_TREE_FIX_MEM

  node *n_new(int x, node* l, node* r, uint h) { 
  	node *p=nodearr+arrtop++;
  	p->item=x;p->left=l;p->right=r;p->height=h;
  	return p;
  }
  void n_delete(node *p) {/*empty now.*/};
#endif
};

AVLtree::AVLtree(DataTable *dt) {
  this->dt=dt;
  root = NULL;
  num_nodes = 0;
#ifdef AVL_TREE_FIX_MEM
  nodearr=new node[dt->GetMaxRows()];
  arrtop=0;
#endif
}

AVLtree::AVLtree(const AVLtree& a) {
  root = copy_nodes(a.root);
  num_nodes = a.num_nodes;
  dt=a.dt;
}

AVLtree& AVLtree::operator=(const AVLtree& a) {
  destroy_nodes(root);
  root = copy_nodes(a.root);
  num_nodes = a.num_nodes;
  dt=a.dt;
  return *this;
}

AVLtree::~AVLtree() {
  destroy_nodes(root);
#ifdef AVL_TREE_FIX_MEM
  delete [] nodearr;
#endif
}

void AVLtree::add_item(int x) {
  root = add_node(root, x);
  num_nodes++;
}

int *AVLtree::search(int x) {
  return search(root, x);
}

int *AVLtree::search(void **ptr) {
  return search(root, ptr);
}

uint AVLtree::size() {
  return num_nodes;
}
/*
void AVLtree::preorder() {
  preorder(root);
}
*/
void AVLtree::inorder(int *obf) {
  int off=0;
  inorder(root,obf,off);
}
/*
void AVLtree::postorder() {
  postorder(root);
}
*/
AVLtree::node* AVLtree::copy_nodes(node *n) {
  if (n == NULL) return NULL;
  node* p;
  node* l;
  node* r;
  l = copy_nodes(n->left);
  r = copy_nodes(n->right);
//  p = new node(n->item, l, r, n->height);
#ifdef AVL_TREE_FIX_MEM
  p = n_new(n->item, l, r, n->height);
#else 
  p = new node(n->item, l, r, n->height);
#endif
  return p;
}

void AVLtree::destroy_nodes(node *n) {
  if (n != NULL) {
    destroy_nodes(n->left);
    destroy_nodes(n->right);
#ifdef AVL_TREE_FIX_MEM
    n_delete(n);
#else
    delete n;
#endif
  }
}

AVLtree::node* AVLtree::add_node(node* n, int x) {
  if (n == NULL) {
    node* p;

#ifdef AVL_TREE_FIX_MEM
    p = n_new(x, NULL, NULL, 1);
#else
    p = new node(x, NULL, NULL, 1);
#endif
    return p;
  } 
  else {
//    if (x < n->item) {
      if (compare(x,n->item)<0) {
      	n->left = add_node(n->left, x);
      	if (height(n->left) - height(n->right) == 2) {
//        if (x < n->left->item) {
	  if (compare(x,n->left->item)<0) {
          	n = rotate_right(n);
          } else {
          	n = rotate2_right(n);
          }
        }
      }
      else {
        n->right = add_node(n->right, x);
        if (height(n->right) - height(n->left) == 2) {
//        if (x >= n->right->item) {
	  if (compare(x,n->right->item)>=0) {
          	n = rotate_left(n);
          } else {
          	n = rotate2_left(n);
          } 
        }
      }
      n->height = 1 + max(height(n->left), height(n->right));
      return n;
  }
}

//uint AVLtree::max(uint a, uint b) {
//  if (a > b) return a;
//  return b;
//}

uint AVLtree::height(node* n) {
  if (n == NULL) return 0;
  return n->height;
}

AVLtree::node* AVLtree::rotate_left(node* n) {
  node* p;
  p = n->right;
  n->right = p->left;
  p->left = n;
  n->height = 1 + max(height(n->left), height(n->right));
  p->height = 1 + max(height(p->left), height(p->right));
  return p;
}

AVLtree::node* AVLtree::rotate2_left(node* n) {
  n->right = rotate_right(n->right);
  n = rotate_left(n);
  return n;
}

AVLtree::node* AVLtree::rotate_right(node* n) {
  node* p;
  p = n->left;
  n->left = p->right;
  p->right = n;
  n->height = 1 + max(height(n->left), height(n->right));
  p->height = 1 + max(height(p->left), height(p->right));
  return p;
}

AVLtree::node* AVLtree::rotate2_right(node* n) {
  n->left = rotate_left(n->left);
  n = rotate_right(n);
  return n;
}

int *AVLtree::search(node* n, int x) {
  if (n == NULL) return NULL;
//  if (n->item == x) return &n->item;
  int rt=compare(n->item ,x);
  if (rt==0) return &n->item;
//  if (n->item > x) {
  if (rt>0) {
    return search(n->left, x);
  } else {
    return search(n->right, x);
  }
}

int *AVLtree::search(node* n, void **ptr) {
  if (n == NULL) return NULL;
//  if (n->item == x) return &n->item;
  int rt=compare(n->item ,ptr);
  if (rt==0) return &n->item;
//  if (n->item > x) {
  if (rt>0) {
    return search(n->left, ptr);
  } else {
    return search(n->right, ptr);
  }
}
/*
void AVLtree::preorder(node* n) {
  if (n != NULL) {
    cout << n->item << " ";
    preorder(n->left);
    preorder(n->right);
  }
}
*/
void AVLtree::inorder(node* n,int *obf,int &off) {
  if (n != NULL) {
    inorder(n->left,obf,off);
	obf[off++]=n->item;
//    cout << n->item << " ";
    inorder(n->right,obf,off);
  }
}
/*
void AVLtree::postorder(node* n) {
  if (n != NULL) {
    postorder(n->left);
    postorder(n->right);
    cout << n->item << " ";
  }
}
*/
#endif
