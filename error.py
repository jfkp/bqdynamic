Traceback (most recent call last):
  File "c:\Users\U50HO63\PROJET\CDP\BenchMark\bq_read_write_analytics_v8.py", line 946, in <module>
    df = run_with_local_files("C:\\Users\\U50HO63\\Downloads\\", local_file_dict)
         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "c:\Users\U50HO63\PROJET\CDP\BenchMark\bq_read_write_analytics_v8.py", line 839, in run_with_local_files
    generate_plots(combined_df, save_plots=True)
  File "c:\Users\U50HO63\PROJET\CDP\BenchMark\bq_read_write_analytics_v8.py", line 794, in generate_plots
    plot_read_write_ratio_analysis(combined_df, save_plots, output_dir)
  File "c:\Users\U50HO63\PROJET\CDP\BenchMark\bq_read_write_analytics_v8.py", line 425, in plot_read_write_ratio_analysis
    ax4.bar(x + i*width*2, read_vals, width, label=f'{tech} READ', alpha=0.7)
  File "C:\Users\U50HO63\PROJET\CDP\BenchMark\bench\Lib\site-packages\matplotlib\__init__.py", line 1524, in inner
    return func(
           ^^^^^
  File "C:\Users\U50HO63\PROJET\CDP\BenchMark\bench\Lib\site-packages\matplotlib\axes\_axes.py", line 2583, in bar
    x, height, width, y, linewidth, hatch = np.broadcast_arrays(
                                            ^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\U50HO63\PROJET\CDP\BenchMark\bench\Lib\site-packages\numpy\lib\_stride_tricks_impl.py", line 544, in broadcast_arrays
    shape = _broadcast_shape(*args)
            ^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\U50HO63\PROJET\CDP\BenchMark\bench\Lib\site-packages\numpy\lib\_stride_tricks_impl.py", line 419, in _broadcast_shape
    b = np.broadcast(*args[:32])
        ^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: shape mismatch: objects cannot be broadcast to a single shape.  Mismatch is between arg 0 with shape (5,) and arg 1 with shape (4,).  
