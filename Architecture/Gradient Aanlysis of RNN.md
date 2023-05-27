<!-- vscode-markdown-toc -->
	* 1. [Background](#Background)
	* 2. [RNN gradient analysis](#RNNgradientanalysis)
		* 2.1. [RNN defined](#RNNdefined)
		* 2.2. [RNN gradient formularized](#RNNgradientformularized)
		* 2.3. [Gradient connectivity](#Gradientconnectivity)

<!-- vscode-markdown-toc-config
	numbering=true
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc -->
Gradient Analysis of RNN

###  1. <a name='Background'></a>Background 

###  2. <a name='RNNgradientanalysis'></a>RNN gradient analysis

This section gives the most clear and detailed analysis of the gradient of RNN.

####  2.1. <a name='RNNdefined'></a>RNN defined

$$\begin{CD} 
\\ @. @V h_0 VV
\\ x_1 @> >> h_1 = f(w, x_1, h_0) @> >> y_1^{'} = g(h_1) @> >> E(y_1^{'}, y_1) @< << y_1 
\\ @. @V h_1 VV 
\\ x_2 @> >> h_2 = f(w, x_2, h_1) @> >> y_2^{'} = g(h_2) @> >> E(y_2^{'}, y_2) @< << y_2 
\\ @. @V h_2 VV
\\ x_3 @> >> h_3 = f(w, x_3, h_2) @> >> y_3^{'} = g(h_3) @> >> E(y_3^{'}, y_3) @< << y_3 
\\ @. @V h_3 VV
\end{CD}$$
<br>

####  2.2. <a name='RNNgradientformularized'></a>RNN gradients formularized

$
\frac{\partial{E}}{\partial{w}}(y_1^{'}, y_1) = 
\frac{\partial{E}}{\partial{y_{}'}}(y_1^{'}, y_1) \frac{\partial{g}}{\partial{h}}(h_1) \frac{\partial{f}}{\partial{w}}(w, x_1, h_0) \\ 
= A_1 \frac{\partial{f}}{\partial{w}}(w, x_1, h_0) \\
= A_1 W_1
$

, where
$
A_1 = \frac{\partial{E}}{\partial{y_{}'}}(y_1^{'}, y_1) \frac{\partial{g}}{\partial{h}}(h_1),
W_1 = \frac{\partial{f}}{\partial{w}}(w, x_1, h_0) 
$
<br>

$
\frac{\partial{E}}{\partial{w}}(y_2^{'}, y_2) 
= \frac{\partial{E}}{\partial{y_{}'}}(y_2^{'}, y_2) \frac{\partial{g}}{\partial{h}}(h_2) ( \frac{\partial{f}}{\partial{w}}(w, x_2, h_1) + \frac{\partial{f}}{\partial{h}}(w, x_2, h_1) \frac{\partial{h_1}}{\partial{w}}(w, x_1, h_0) ) \\
= A_2 ( W_2 + \frac{\partial{f}}{\partial{h}}(w, x_2, h_1) \frac{\partial{h_1}}{\partial{w}}(w, x_1, h_0) ) \\
= A_2 ( W_2 + H_1 \frac{\partial{f}}{\partial{w}}(w, x_1, h_0) ) \\
= A_2 (W_2 + H_1 W_1) 
$

, where
$
A_2 = \frac{\partial{E}}{\partial{y_{}'}}(y_2^{'}, y_2) \frac{\partial{g}}{\partial{h}}(h_2), 
W_2 = \frac{\partial{f}}{\partial{w}}(w, x_2, h_1),
H_1 = \frac{\partial{f}}{\partial{h}}(w, x_2, h_1). 
$
<br>

$
\frac{\partial{E}}{\partial{w}}(y_3^{'}, y_3) 
= \frac{\partial{E}}{\partial{y_{}'}}(y_3^{'}, y_3) \frac{\partial{g}}{\partial{h}}(h_3) ( \frac{\partial{f}}{\partial{w}}(w, x_3, h_2) + \frac{\partial{f}}{\partial{h}}(w, x_3, h_2) \frac{\partial{h_2}}{\partial{w}}(w, x_1..x_2, h_0) ) \\
= A_3 ( W_3 + H_2 \frac{\partial{h_2}}{\partial{w}}(w, x_1..x_2) ) \\
= A_3 ( W_3 + H_2 (W_2 + H_1 W_1) ) \\
= A_3 ( W_3 + H_2 W_2 + H_2 H_1 W_1 ) 
$

, where 
$
A_3 = \frac{\partial{E}}{\partial{y_{}'}}(y_3^{'}, y_3) \frac{\partial{g}}{\partial{h}}(h_3),
W_3 = \frac{\partial{f}}{\partial{w}}(w, x_3, h_2),
H_2 = \frac{\partial{f}}{\partial{h}}(w, x_3, h_2). 
$
<br>

We can deduce the following equation, with mathematical induction:

$
\frac{\partial{E}}{\partial{w}}(y_n^{'}, y_n) 
= A_n \sum_{i=n}^{1} W_i C_{i,n}
$

, where
$
A_n = \frac{\partial{E}}{\partial{y_{}'}}(y_n^{'}, y_n) \frac{\partial{g}}{\partial{h}} (h_n), 
W_n = \frac{\partial{f}}{\partial{w}}(w, x_n, h_{n-1}), 
C_{i, n} = \prod_{k=i}^{n-1} H_k,
H_k = \frac{\partial{f}}{\partial{h}}(w, x_{k+1}, h_k)
$
<br>

The intuition of this equation, which helps greatly to investigate the nature of so-called "gradient vanish", is as follows:
- $W_i = \frac{\partial{f}}{\partial{w}}(w, x_i, h_{i-1})$: $w$'s local action at the step $i$, which is the gradient of f w.r.t. the weights $w$ at the $i$-th step of recursion. This is how the weights $w$ work locally at that step.
- $\prod_{k=i}^{n-1} H_k$: The connectivity that adapts $w$'s local action at the $i$-th step to the $n$-th step of recursion.
- $A_n$: The connectivity that adapts past $w$'s $n$-th local action to the $n$-th cost of RNN.
- So, the $n$-the cost of RNN is affected by all previous local actions of $w$, in parallel, through their respective connectivities.


####  2.3. <a name='Gradientconnectivity'></a>Gradient connectivity


$$\begin{CD} 
\\ @. @V h_0, y_0 VV
\\ x_1 @> >> h_1 = f(w, x_1, h_0), y_0 @> >> y_1^{'} = g(h_1), y_0 @> >> E(y_1^{'}, y_1, y_0) @< << y_1 
\\ @. @V h_1, y_1 VV 
\\ x_2 @> >> h_2 = f(w, x_2, h_1), y_1 @> >> y_2^{'} = g(h_2), y_1 @> >> E(y_2^{'}, y_2, y_1) @< << y_2 
\\ @. @V h_2, y_2 VV
\\ x_3 @> >> h_3 = f(w, x_3, h_2), y_2 @> >> y_3^{'} = g(h_3), y_2 @> >> E(y_3^{'}, y_3, y_2) @< << y_3 
\\ @. @V h_3, y_3 VV
\end{CD}$$
<br>