import tkinter as tk
from tkinter import ttk
from tkinter import END
from pi_sql_interface import Database


class FinanceForm(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)

        self.database = Database()
        self.database.connect()

        self.title("Finance Form")

        self.operating_cashflow = tk.StringVar()
        self.cap_expenditure = tk.StringVar()
        self.freecashflow = tk.StringVar()
        self.ticker_variable = tk.StringVar()

        ticker_options = self.database.query('SELECT t.id as id, (SELECT COUNT(1) FROM cashflow c WHERE c.id=t.id) AS datacount FROM ticker t ORDER BY t.id ASC')
        ticker_options = ["[{}]  {}".format(ticker[1], ticker[0]) for ticker in ticker_options]

        self.ticker_variable.set("Select an item..")

        self.ticker_label = tk.Label(text="Ticker")
        self.ticker_label.grid(row=0, column=0, padx=10, pady=10)

        self.ticker_dropdown = ttk.Combobox(textvariable=self.ticker_variable, values=ticker_options)
        self.ticker_dropdown.grid(row=0, column=1, padx=10, pady=10)

        self.operating_cashflow_entry = ttk.Entry(self, textvariable=self.operating_cashflow)
        self.cap_expenditure_entry = ttk.Entry(self, textvariable=self.cap_expenditure)
        self.freecashflow_entry = ttk.Entry(self, textvariable=self.freecashflow)

        self.operating_cashflow_label = ttk.Label(self, text="Operating Cashflow:")
        self.cap_expenditure_label = ttk.Label(self, text="Capital Expenditure:")
        self.freecashflow_label = ttk.Label(self, text="Free Cashflow:")

        self.calculate_oper_btn = ttk.Button(self, text=">", command=self.calculate_oper)
        self.calculate_capex_btn = ttk.Button(self, text=">", command=self.calculate_capex)
        self.calculate_fcf_btn = ttk.Button(self, text=">", command=self.calculate_fcf)

        self.operating_cashflow_label.grid(row=1, column=0)
        self.cap_expenditure_label.grid(row=2, column=0)
        self.freecashflow_label.grid(row=3, column=0)

        self.operating_cashflow_entry.grid(row=1, column=1)
        self.cap_expenditure_entry.grid(row=2, column=1)
        self.freecashflow_entry.grid(row=3, column=1)

        self.calculate_oper_btn.grid(row=1, column=2)
        self.calculate_capex_btn.grid(row=2, column=2)
        self.calculate_fcf_btn.grid(row=3, column=2)

        tk.Label(self, text="").grid(row=4, column=0, pady=20, padx=5)
        self.shapes_list = ttk.Treeview(self, columns=("Oper", "CapEx", "FCF",))
        self.shapes_list.heading("#0", text="Year")
        self.shapes_list.heading("Oper", text="Oper")
        self.shapes_list.heading("CapEx", text="CapEx")
        self.shapes_list.heading("FCF", text="FCF")
        self.shapes_list.grid(row=5, column=0, columnspan=15, pady=5, padx=5)

        self.ticker_variable.trace("w", self.ticker_change)

        #self.shapes_list.bind("<Button-1>", self.cashflow_selected)
        self.shapes_list.bind("<ButtonRelease-1>", self.cashflow_selected)

    def ticker_change(self, *args):
        ticker = self.ticker_variable.get().rsplit(" ", 1)[-1]

        sql = "SELECT year, operating_cashflow, cap_expenditure, freecashflow FROM cashflow WHERE id='{0}'"
        cashflow_results = self.database.query(sql.format(ticker))

        if len(self.shapes_list.get_children()) > 0:
            for row in self.shapes_list.get_children():
                self.shapes_list.delete(row)

        if ticker:
            for data in cashflow_results:
                self.shapes_list.insert("", "end", text=data[0], values=(1000*data[1], 1000*data[2], 1000*data[3],))

    def calculate_oper(self):
        try:
            cap_expenditure = int(self.cap_expenditure.get())
            freecashflow = int(self.freecashflow.get())
            operating_cashflow = freecashflow - cap_expenditure
            self.operating_cashflow.set(operating_cashflow)
        except ValueError:
            pass

    def calculate_capex(self):
        try:
            operating_cashflow = int(self.operating_cashflow.get())
            freecashflow = int(self.freecashflow.get())
            cap_expenditure = freecashflow - operating_cashflow
            self.cap_expenditure.set(cap_expenditure)
        except ValueError:
            pass

    def calculate_fcf(self):
        try:
            operating_cashflow = int(self.operating_cashflow.get())
            cap_expenditure = int(self.cap_expenditure.get())
            freecashflow = operating_cashflow + cap_expenditure
            self.freecashflow.set(freecashflow)
        except ValueError:
            pass

    def cashflow_selected(self, event):
        item = self.shapes_list.focus()
        print(self.shapes_list.item(item))
        year = self.shapes_list.item(item)["text"]
        cashflow = self.shapes_list.item(item)["values"]

        self.operating_cashflow.set(cashflow[0])
        self.cap_expenditure.set(cashflow[1])
        self.freecashflow.set(cashflow[2])

class SelectTicker:
    def __init__(self, master):
        # define root frame object
        self.master = master
        # define database
        self.database = Database()
        self.database.connect()

        self.master.geometry("400x400")

        # get ticker list
        sql = 'SELECT t.id as id, (SELECT COUNT(1) FROM cashflow c WHERE c.id=t.id) AS datacount FROM ticker t ORDER BY t.id ASC'
        self.ticker_options = self.database.query(sql)
        #ticker_options = ["[{}]  {}".format(ticker[1], ticker[0]) for ticker in ticker_options]
        #print(ticker_options)

        self.tree = ttk.Treeview(self.master)
        self.tree["columns"] = ("one")
        self.tree.column("one", width=100)
        self.tree.heading("one", text="DataEntries")

        for ticker in self.ticker_options:
            self.tree.insert("", "end", text=ticker[0], values=(ticker[1]))

        self.tree.pack(fill='both', expand=True)

        self.filter_label = ttk.Label(self.master, text="Filter")
        self.filter_entry = tk.Entry(self.master)
        self.filter_entry.pack(pady=10)

        self.filter_entry.bind("<KeyRelease>", lambda e: self.filter_tree(self.filter_entry))

        self.selected_item = tk.StringVar()
        self.selected_item_label = tk.Label(self.master, textvariable=self.selected_item)
        self.selected_item_label.pack(pady=10)

        self.tree.bind("<ButtonRelease-1>", self.show_selected_item)

        button_frame = tk.Frame(self.master)
        button_frame.pack(pady=10)

        self.ok_button = tk.Button(button_frame, text="OK", command=self.ok, width=10)
        self.ok_button.pack(side="left", padx=10)

        self.cancel_button = tk.Button(button_frame, text="Cancel", command=self.cancel, width=10)
        self.cancel_button.pack(side="left", padx=10)
        root.protocol("WM_DELETE_WINDOW", self.cancel)

    def ok(self):
        self.master.destroy()

    def cancel(self):
        self.selected_item.set("")
        self.master.destroy()

    def filter_tree(self, entry):
        for i in self.tree.get_children():
            self.tree.delete(i)
        for ticker in self.ticker_options:
            if entry.get().lower() in ticker[0].lower():
                self.tree.insert("", "end", text=ticker[0], values=(ticker[1]))

    def show_selected_item(self, event):
        self.selected_item.set(self.tree.item(self.tree.focus())["text"])

    def show(self):
        self.master.mainloop()
        return self.selected_item.get()

if __name__ == "__main__":
    #app = FinanceForm()
    #app.mainloop()

    root = tk.Tk()

    GetTicker = SelectTicker(root)
    ticker = GetTicker.show()
    print(ticker)
    print(GetTicker.selected_item.get())